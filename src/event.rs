use crate::definitions::*;
use crate::error::ParseError;
use crate::parser::Decoder;

#[derive(Debug)]
pub enum Event {
    TextMessage(TextMessageEvent),
    ClientMoved(ClientMoveEvent),
    ClientEnterView(ClientEnterViewEvent),
    ClientLeftView(ClientLeftViewEvent),
    ClientUpdated(ClientUpdatedEvent),
    ChannelCreated(ChannelCreateEvent),
    ChannelDeleted(ChannelDeleteEvent),
    ChannelEdited(ChannelEditEvent),
    ChannelMoved(ChannelMoveEvent),
    ChannelDescriptionChanged(ChannelDescriptionChangeEvent),
    ChannelPasswordChanged(ChannelPasswordChangeEvent),
    ServerEdited(ServerEditEvent),
    TokenUsed(TokenUseEvent),
}

impl Event {
    pub fn from(response: &str) -> Result<Self, ParseError> {
        let mut decoder = Decoder::new(response.as_bytes());
        let name = decoder.decode_name()?;

        Ok(match name.as_str() {
            "notifytextmessage" => Event::TextMessage(decoder.decode()?),
            "notifyclientmoved" => Event::ClientMoved(decoder.decode()?),
            "notifycliententerview" => Event::ClientEnterView(decoder.decode()?),
            "notifyclientleftview" => Event::ClientLeftView(decoder.decode()?),
            "notifyclientupdated" => Event::ClientUpdated(decoder.decode()?),
            "notifychannelcreated" => Event::ChannelCreated(decoder.decode()?),
            "notifychanneldeleted" => Event::ChannelDeleted(decoder.decode()?),
            "notifychanneledited" => Event::ChannelEdited(decoder.decode()?),
            "notifychannelmoved" => Event::ChannelMoved(decoder.decode()?),
            "notifychanneldescriptionchanged" => {
                Event::ChannelDescriptionChanged(decoder.decode()?)
            }
            "notifychannelpasswordchanged" => Event::ChannelPasswordChanged(decoder.decode()?),
            "notifyserveredited" => Event::ServerEdited(decoder.decode()?),
            "notifytokenused" => Event::TokenUsed(decoder.decode()?),
            _ => {
                return Err(ParseError::UnknownEvent {
                    response: response.to_string(),
                    event: name.clone(),
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_partial_client_updated() {
        // The server sends only the changed properties plus `clid`.
        let event = Event::from("notifyclientupdated clid=7 client_input_muted=1").unwrap();
        match event {
            Event::ClientUpdated(e) => {
                assert_eq!(e.client_id, 7);
                assert_eq!(e.input_muted, Some(true));
                assert_eq!(e.output_muted, None);
                assert_eq!(e.away, None);
            }
            other => panic!("expected ClientUpdated, got {other:?}"),
        }
    }

    #[test]
    fn parses_client_updated_away() {
        let event =
            Event::from("notifyclientupdated clid=42 client_away=1 client_away_message=brb").unwrap();
        match event {
            Event::ClientUpdated(e) => {
                assert_eq!(e.client_id, 42);
                assert_eq!(e.away, Some(true));
                assert_eq!(e.away_message.as_deref(), Some("brb"));
                assert_eq!(e.input_muted, None);
            }
            other => panic!("expected ClientUpdated, got {other:?}"),
        }
    }
}
