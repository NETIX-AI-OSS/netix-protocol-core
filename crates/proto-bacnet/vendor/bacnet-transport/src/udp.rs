//! UDP socket helpers shared by the BACnet/IP and BACnet/IPv6 receive loops (a NETIX patch to the vendored crate).
//!
//! When a datagram the socket sent draws an ICMP port-unreachable (a device that went away, a Who-Is
//! to a closed port), Windows fails the socket's next `recv_from` with `WSAECONNRESET`. The receive
//! loops used to stop on any error, so one unreachable peer silently ended all BACnet/IP reception.
//! [`ignore_connection_reset`] switches that reporting off (`SIO_UDP_CONNRESET`) and
//! [`is_transient`] lets the loops continue past the errors that remain. Linux reports ICMP errors
//! only on connected sockets, so both are no-ops in practice there.

use std::io;

/// Stop Windows from reporting ICMP port-unreachable as `WSAECONNRESET` on later receives; a no-op elsewhere.
#[cfg(windows)]
#[allow(unsafe_code)]
pub(crate) fn ignore_connection_reset(socket: &socket2::Socket) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        WSAGetLastError, WSAIoctl, SIO_UDP_CONNRESET, SOCKET, SOCKET_ERROR,
    };
    let off: u32 = 0;
    let mut returned = 0u32;
    // SAFETY: a valid socket owned by the caller; the input buffer is the 4-byte BOOL FALSE.
    let rc = unsafe {
        WSAIoctl(
            socket.as_raw_socket() as SOCKET,
            SIO_UDP_CONNRESET,
            (&off as *const u32).cast(),
            4,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
            None,
        )
    };
    if rc == SOCKET_ERROR {
        // SAFETY: no arguments.
        return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
    }
    Ok(())
}

/// Stop Windows from reporting ICMP port-unreachable as `WSAECONNRESET` on later receives; a no-op elsewhere.
#[cfg(not(windows))]
pub(crate) fn ignore_connection_reset(_socket: &socket2::Socket) -> io::Result<()> {
    Ok(())
}

/// Receive errors a loop should log and continue past: a reset or refusal reported for an earlier
/// datagram, an interrupted call, and (Windows) a datagram larger than the buffer, which fails with
/// `WSAEMSGSIZE` where Linux truncates.
pub(crate) fn is_transient(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionReset | ConnectionRefused | ConnectionAborted | Interrupted | WouldBlock | TimedOut
    ) || (cfg!(windows) && matches!(e.raw_os_error(), Some(10040 | 10051 | 10052 | 10054 | 10065)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resets_and_refusals_are_transient_permission_errors_are_not() {
        assert!(is_transient(&io::Error::from(io::ErrorKind::ConnectionReset)));
        assert!(is_transient(&io::Error::from(io::ErrorKind::ConnectionRefused)));
        assert!(!is_transient(&io::Error::from(io::ErrorKind::PermissionDenied)));
        assert_eq!(is_transient(&io::Error::from_raw_os_error(10040)), cfg!(windows));
    }

    #[test]
    fn udp_sockets_accept_the_switch() {
        let s = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None).unwrap();
        ignore_connection_reset(&s).unwrap();
    }
}
