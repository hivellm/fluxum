//! `tagged_enum!` — MessagePack tagged-variant enums.
//!
//! Every generated enum encodes as the RPC-011 tagged pattern: a
//! `fixarray[2]` of `[tag: str, payload]`. This is the envelope
//! representation for [`ClientMessage`](crate::messages::ClientMessage),
//! [`ServerMessage`](crate::messages::ServerMessage), and
//! [`RowSizeHint`](crate::rowlist::RowSizeHint).

/// Define an enum whose serde representation is `["Tag", payload]`
/// (MessagePack `fixarray[2]`), plus `encode`/`decode` helpers using
/// `rmp-serde`.
macro_rules! tagged_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$vmeta:meta])* $tag:literal => $variant:ident($payload:ty) ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        pub enum $name {
            $( $(#[$vmeta])* $variant($payload), )+
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeTuple;
                match self {
                    $(
                        Self::$variant(payload) => {
                            let mut tuple = serializer.serialize_tuple(2)?;
                            tuple.serialize_element($tag)?;
                            tuple.serialize_element(payload)?;
                            tuple.end()
                        }
                    )+
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct TagVisitor;

                impl<'de> serde::de::Visitor<'de> for TagVisitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str(concat!(
                            "a 2-element [tag, payload] array encoding a ",
                            stringify!($name),
                        ))
                    }

                    fn visit_seq<A: serde::de::SeqAccess<'de>>(
                        self,
                        mut seq: A,
                    ) -> Result<Self::Value, A::Error> {
                        let tag: std::string::String = seq
                            .next_element()?
                            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                        match tag.as_str() {
                            $(
                                $tag => Ok($name::$variant(
                                    seq.next_element()?
                                        .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?,
                                )),
                            )+
                            other => Err(serde::de::Error::unknown_variant(other, &[$( $tag ),+])),
                        }
                    }
                }

                deserializer.deserialize_tuple(2, TagVisitor)
            }
        }

        impl $name {
            /// Encode to a MessagePack body (envelope only, no frame header).
            pub fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
                rmp_serde::to_vec(self)
            }

            /// Decode from a MessagePack body (envelope only, no frame header).
            pub fn decode(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
                rmp_serde::from_slice(bytes)
            }
        }
    };
}

pub(crate) use tagged_enum;
