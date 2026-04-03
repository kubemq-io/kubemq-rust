/// Connection state machine.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Idle,
    Ready,
    Closed,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Ready => write!(f, "Ready"),
            Self::Closed => write!(f, "Closed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_values() {
        assert_ne!(ConnectionState::Idle, ConnectionState::Ready);
        assert_ne!(ConnectionState::Ready, ConnectionState::Closed);
        assert_ne!(ConnectionState::Closed, ConnectionState::Idle);
    }

    // -- ConnectionState Display tests --

    #[test]
    fn test_connection_state_display_idle() {
        assert_eq!(format!("{}", ConnectionState::Idle), "Idle");
    }

    #[test]
    fn test_connection_state_display_ready() {
        assert_eq!(format!("{}", ConnectionState::Ready), "Ready");
    }

    #[test]
    fn test_connection_state_display_closed() {
        assert_eq!(format!("{}", ConnectionState::Closed), "Closed");
    }

    // -- ConnectionState Clone and Copy tests --

    #[test]
    fn test_connection_state_clone() {
        let s = ConnectionState::Ready;
        let s2 = s;
        assert_eq!(s, s2);
    }

    #[test]
    fn test_connection_state_debug() {
        let debug = format!("{:?}", ConnectionState::Ready);
        assert!(debug.contains("Ready"));
    }
}
