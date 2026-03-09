# UDP Transport — Command Processor Wiring

## File: `core/src/socket/core/command_processor.rs`

Four new match arms are needed: two in `handle_user_bind` and two in
`handle_user_connect` (one per socket role in each operation).

### Socket-type Guard

Both bind and connect must reject non-Radio/non-Dish sockets immediately:

```rust
fn udp_socket_type_check(socket_type: SocketType, uri: &str) -> Result<(), ZmqError> {
    match socket_type {
        SocketType::Radio | SocketType::Dish => Ok(()),
        _ => Err(ZmqError::UnsupportedTransport(format!(
            "udp:// is only valid for Radio and Dish sockets, got {:?} on {}",
            socket_type, uri
        ))),
    }
}
```

---

## `handle_user_bind` — `Endpoint::Udp` arm

```rust
#[cfg(feature = "udp")]
Ok(Endpoint::Udp(ref udp_ep, ref uri)) => {
    // 1. Type check
    let socket_type = core_arc.core_state.read().socket_type;
    if let Err(e) = udp_socket_type_check(socket_type, uri) {
        bind_result = Err(e);
    }
    // 2. Duplicate bind check
    else if core_arc.core_state.read().endpoints.contains_key(uri) {
        bind_result = Err(ZmqError::AddrInUse(uri.clone()));
    }
    else {
        let options = core_arc.core_state.read().options.clone();
        let monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();

        match socket_type {
            // ── RADIO bind ────────────────────────────────────────────────
            // Creates a bound outgoing UDP socket. No receive loop spawned.
            SocketType::Radio => {
                match udp::create_bound_send_socket(udp_ep, &options) {
                    Ok((udp_sock, resolved_uri)) => {
                        let conn_id = context_clone.inner().next_handle();
                        let udp_arc = Arc::new(udp_sock);
                        let conn: Arc<dyn ISocketConnection> = Arc::new(
                            UdpSendConnection::new(
                                udp_arc,
                                udp_ep.send_addr,   // for Radio bind this is unused
                                conn_id,
                            )
                        );

                        // Allocate synthetic pipe IDs (write + read)
                        // Radio bind doesn't receive, but pipe_read_id is needed
                        // for the EndpointInfo structure.
                        let pipe_write_id = context_clone.inner().next_handle();
                        let pipe_read_id  = context_clone.inner().next_handle();

                        {
                            let mut cs = core_arc.core_state.write();
                            cs.pipe_read_id_to_endpoint_uri
                              .insert(pipe_read_id, resolved_uri.clone());
                            cs.endpoints.insert(
                                resolved_uri.clone(),
                                EndpointInfo {
                                    mailbox: core_arc.command_sender(),
                                    task_handle: None,          // no background task
                                    endpoint_type: EndpointType::Session,
                                    endpoint_uri: resolved_uri.clone(),
                                    pipe_ids: Some((pipe_write_id, pipe_read_id)),
                                    handle_id: conn_id,
                                    target_endpoint_uri: None,
                                    is_outbound_connection: false,
                                    peer_socket_type: None,
                                    connection_iface: conn,
                                },
                            );
                        }

                        socket_logic.pipe_attached(pipe_read_id, pipe_write_id, None).await;

                        if let Some(ref mtx) = monitor_tx {
                            let _ = mtx.try_send(SocketEvent::Listening {
                                endpoint: resolved_uri.clone(),
                            });
                        }

                        actual_uri_for_state_update = Some(resolved_uri);
                        bind_result = Ok(());
                    }
                    Err(e) => bind_result = Err(e),
                }
            }

            // ── DISH bind ────────────────────────────────────────────────
            // Spawns a UdpReceiveActor that owns the receiving socket.
            SocketType::Dish => {
                let child_handle = context_clone.inner().next_handle();
                let pipe_read_id = context_clone.inner().next_handle();
                let pipe_write_id = context_clone.inner().next_handle();

                match udp::UdpReceiveActor::create_and_spawn(
                    child_handle,
                    udp_ep,
                    socket_logic.clone(),
                    context_clone.clone(),
                    parent_socket_id,
                    monitor_tx.clone(),
                    core_arc.clone(),
                    pipe_read_id,
                    &options,
                ) {
                    Ok((actor_mailbox, task_handle, resolved_uri)) => {
                        // UdpReceiveActor is receive-only; send side is a DummyConnection.
                        let conn: Arc<dyn ISocketConnection> =
                            Arc::new(DummyConnection);

                        {
                            let mut cs = core_arc.core_state.write();
                            cs.pipe_read_id_to_endpoint_uri
                              .insert(pipe_read_id, resolved_uri.clone());
                            cs.endpoints.insert(
                                resolved_uri.clone(),
                                EndpointInfo {
                                    mailbox: actor_mailbox,
                                    task_handle: Some(task_handle),
                                    endpoint_type: EndpointType::Listener,
                                    endpoint_uri: resolved_uri.clone(),
                                    pipe_ids: Some((pipe_write_id, pipe_read_id)),
                                    handle_id: child_handle,
                                    target_endpoint_uri: None,
                                    is_outbound_connection: false,
                                    peer_socket_type: None,
                                    connection_iface: conn,
                                },
                            );
                        }

                        socket_logic.pipe_attached(pipe_read_id, pipe_write_id, None).await;

                        actual_uri_for_state_update = Some(resolved_uri);
                        bind_result = Ok(());
                    }
                    Err(e) => bind_result = Err(e),
                }
            }

            _ => unreachable!("type check above ensures only Radio/Dish reach here"),
        }
    }
}
```

---

## `handle_user_connect` — `Endpoint::Udp` arm

UDP connections are **not** routed through `respawn_connecter_actor` — there is
no TCP-style reconnect loop. The connection is registered synchronously.

```rust
#[cfg(feature = "udp")]
Ok(Endpoint::Udp(ref udp_ep, ref uri)) => {
    // 1. Type check
    let socket_type = core_arc.core_state.read().socket_type;
    if let Err(e) = udp_socket_type_check(socket_type, uri) {
        let _ = reply_tx.send(Err(e));
        return;
    }

    // 2. Duplicate connect check
    if core_arc.core_state.read().endpoints.contains_key(uri) {
        let _ = reply_tx.send(Err(ZmqError::AddrInUse(uri.clone())));
        return;
    }

    let options = core_arc.core_state.read().options.clone();
    let monitor_tx = core_arc.core_state.read().get_monitor_sender_clone();
    let context_clone = core_arc.context.clone();
    let parent_socket_id = core_arc.handle;

    match socket_type {
        // ── RADIO connect ─────────────────────────────────────────────
        // Creates an unbound (or SO_REUSEADDR-set) UDP socket that sends
        // datagrams to the specified remote address.
        SocketType::Radio => {
            match udp::create_connected_send_socket(udp_ep, &options) {
                Ok(udp_sock) => {
                    let conn_id = context_clone.inner().next_handle();
                    let udp_arc = Arc::new(udp_sock);
                    let resolved_uri = format!("udp://{}", udp_ep.send_addr);

                    let conn: Arc<dyn ISocketConnection> = Arc::new(
                        UdpSendConnection::new(udp_arc, udp_ep.send_addr, conn_id)
                    );

                    let pipe_write_id = context_clone.inner().next_handle();
                    let pipe_read_id  = context_clone.inner().next_handle();

                    {
                        let mut cs = core_arc.core_state.write();
                        cs.pipe_read_id_to_endpoint_uri
                          .insert(pipe_read_id, resolved_uri.clone());
                        cs.endpoints.insert(
                            resolved_uri.clone(),
                            EndpointInfo {
                                mailbox: core_arc.command_sender(),
                                task_handle: None,
                                endpoint_type: EndpointType::Session,
                                endpoint_uri: resolved_uri.clone(),
                                pipe_ids: Some((pipe_write_id, pipe_read_id)),
                                handle_id: conn_id,
                                target_endpoint_uri: Some(uri.clone()),
                                is_outbound_connection: true,
                                peer_socket_type: None,
                                connection_iface: conn,
                            },
                        );
                    }

                    socket_logic.pipe_attached(pipe_read_id, pipe_write_id, None).await;

                    if let Some(ref mtx) = monitor_tx {
                        let _ = mtx.try_send(SocketEvent::Connected {
                            endpoint: resolved_uri.clone(),
                            peer_addr: udp_ep.send_addr.to_string(),
                        });
                    }

                    let _ = reply_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = reply_tx.send(Err(e));
                }
            }
        }

        // ── DISH connect ──────────────────────────────────────────────
        // Spawns a UdpReceiveActor bound to 0.0.0.0:0.
        // In connect mode the actor receives from any sender (libzmq behaviour).
        // The send_addr from udp_ep is stored but not used for filtering here;
        // application-level filtering can be added later if needed.
        SocketType::Dish => {
            let child_handle = context_clone.inner().next_handle();
            let pipe_read_id  = context_clone.inner().next_handle();
            let pipe_write_id = context_clone.inner().next_handle();

            // For Dish connect: use bind_addr 0.0.0.0:0 (OS picks ephemeral port)
            // Override the endpoint's bind_addr before passing to the actor.
            let mut connect_ep = udp_ep.clone();
            connect_ep.bind_addr = if udp_ep.is_ipv6 {
                "[::]:0".parse().unwrap()
            } else {
                "0.0.0.0:0".parse().unwrap()
            };

            match udp::UdpReceiveActor::create_and_spawn(
                child_handle,
                &connect_ep,
                socket_logic.clone(),
                context_clone.clone(),
                parent_socket_id,
                monitor_tx.clone(),
                core_arc.clone(),
                pipe_read_id,
                &options,
            ) {
                Ok((actor_mailbox, task_handle, resolved_uri)) => {
                    let conn: Arc<dyn ISocketConnection> = Arc::new(DummyConnection);

                    {
                        let mut cs = core_arc.core_state.write();
                        cs.pipe_read_id_to_endpoint_uri
                          .insert(pipe_read_id, resolved_uri.clone());
                        cs.endpoints.insert(
                            resolved_uri.clone(),
                            EndpointInfo {
                                mailbox: actor_mailbox,
                                task_handle: Some(task_handle),
                                endpoint_type: EndpointType::Session,
                                endpoint_uri: resolved_uri.clone(),
                                pipe_ids: Some((pipe_write_id, pipe_read_id)),
                                handle_id: child_handle,
                                target_endpoint_uri: Some(uri.clone()),
                                is_outbound_connection: true,
                                peer_socket_type: None,
                                connection_iface: conn,
                            },
                        );
                    }

                    socket_logic.pipe_attached(pipe_read_id, pipe_write_id, None).await;

                    if let Some(ref mtx) = monitor_tx {
                        let _ = mtx.try_send(SocketEvent::Connected {
                            endpoint: resolved_uri.clone(),
                            peer_addr: udp_ep.send_addr.to_string(),
                        });
                    }

                    let _ = reply_tx.send(Ok(()));
                }
                Err(e) => {
                    let _ = reply_tx.send(Err(e));
                }
            }
        }

        _ => unreachable!(),
    }
}
```

---

## `respawn_connecter_actor` — no change

The `_ =>` fallback arm at the end already emits a warning for unsupported
transports. UDP endpoints will never reach `respawn_connecter_actor` because
`handle_user_connect` handles them inline and returns before calling it.

---

## Helper Functions in `udp.rs`

```rust
/// Creates an unbound UDP socket configured for sending only.
/// Used by Radio::connect.
pub(crate) fn create_connected_send_socket(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<tokio::net::UdpSocket, ZmqError>

/// Creates a bound UDP socket with no receive loop.
/// Used by Radio::bind.
pub(crate) fn create_bound_send_socket(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<(tokio::net::UdpSocket, String), ZmqError>
//          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^
//          tokio socket ready to send    resolved "udp://addr:port" URI
```

Both share the same socket setup sequence (see 03_new_files.md, steps 1–6).
The difference is only in whether `bind()` is called and what address is bound.

---

## Shutdown / Cleanup

No special handling needed. `cleanup_stopped_child_resources` already handles
the general `EndpointType::Session` and `EndpointType::Listener` cleanup paths:

- Radio bind / Radio connect: `EndpointType::Session`, no task to abort,
  `close_connection()` on `UdpSendConnection` is a no-op. Cleanup is instant.
- Dish bind / Dish connect: `EndpointType::Listener` / `EndpointType::Session`,
  `task_handle.abort()` stops the `UdpReceiveActor`. `close_connection()` on
  `DummyConnection` is a no-op.
- `pipe_detached` is called on the `ISocket` in all cases, matching TCP behaviour.

No reconnect logic is added for UDP. The `should_consider_reconnect` flag in
`cleanup_stopped_child_resources` will never be true for UDP endpoints because:
- `is_outbound_connection` is `false` for Radio bind and Dish bind.
- `is_outbound_connection` is `true` for Radio connect and Dish connect, but
  `reconnect_ivl` is `None` for those (no reconnect configured).
- Even if reconnect were configured, `crate::transport::tcp::is_fatal_connect_error`
  is not applicable to UDP; a separate guard can be added later if needed.
