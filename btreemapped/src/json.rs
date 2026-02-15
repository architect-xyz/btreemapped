/// Wrapper type for JSON fields that implements ToSql by serializing to serde_json::Value
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgJson<T>(pub T);

impl<T> std::ops::Deref for PgJson<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for PgJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> std::fmt::Display for PgJson<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> serde::Serialize for PgJson<T>
where
    T: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, T> serde::Deserialize<'de> for PgJson<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(PgJson)
    }
}

impl<T> postgres_types::ToSql for PgJson<T>
where
    T: serde::Serialize + std::fmt::Debug + Sync,
{
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let json_value = serde_json::to_value(&self.0)?;
        json_value.to_sql(ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        matches!(*ty, postgres_types::Type::JSON | postgres_types::Type::JSONB)
    }

    postgres_types::to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deref() {
        let pj = PgJson(42);
        assert_eq!(*pj, 42);
    }

    #[test]
    fn test_deref_mut() {
        let mut pj = PgJson(42);
        *pj = 100;
        assert_eq!(pj.0, 100);
    }

    #[test]
    fn test_display() {
        let pj = PgJson("hello".to_string());
        assert_eq!(format!("{}", pj), "hello");
    }

    #[test]
    fn test_serialize() {
        let pj = PgJson(vec![1, 2, 3]);
        let json = serde_json::to_string(&pj).unwrap();
        assert_eq!(json, "[1,2,3]");
    }

    #[test]
    fn test_deserialize() {
        let pj: PgJson<Vec<i32>> = serde_json::from_str("[1,2,3]").unwrap();
        assert_eq!(pj.0, vec![1, 2, 3]);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();
        map.insert("key".to_string(), 42);
        let pj = PgJson(map);
        let json = serde_json::to_string(&pj).unwrap();
        let pj2: PgJson<BTreeMap<String, i32>> =
            serde_json::from_str(&json).unwrap();
        assert_eq!(pj, pj2);
    }

    #[test]
    fn test_to_sql_json() {
        use bytes::BytesMut;
        use postgres_types::ToSql;

        let pj = PgJson(serde_json::json!({"key": "value"}));
        let mut buf = BytesMut::new();
        pj.to_sql(&postgres_types::Type::JSON, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_to_sql_jsonb() {
        use bytes::BytesMut;
        use postgres_types::ToSql;

        let pj = PgJson(serde_json::json!({"key": "value"}));
        let mut buf = BytesMut::new();
        pj.to_sql(&postgres_types::Type::JSONB, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_accepts() {
        use postgres_types::ToSql;
        assert!(PgJson::<serde_json::Value>::accepts(
            &postgres_types::Type::JSON
        ));
        assert!(PgJson::<serde_json::Value>::accepts(
            &postgres_types::Type::JSONB
        ));
        assert!(!PgJson::<serde_json::Value>::accepts(
            &postgres_types::Type::TEXT
        ));
    }

    #[test]
    fn test_eq_ord() {
        let a = PgJson(1);
        let b = PgJson(2);
        let c = PgJson(1);
        assert_eq!(a, c);
        assert_ne!(a, b);
        assert!(a < b);
    }

    #[test]
    fn test_clone_debug() {
        let pj = PgJson(42);
        let cloned = pj.clone();
        assert_eq!(pj, cloned);
        let debug = format!("{:?}", pj);
        assert!(debug.contains("42"));
    }
}

#[cfg(feature = "sqlx")]
impl<'r, T> sqlx::Decode<'r, sqlx::Postgres> for PgJson<T>
where
    T: serde::de::DeserializeOwned,
{
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<PgJson<T>, Box<dyn std::error::Error + 'static + Send + Sync>> {
        use sqlx::ValueRef;

        if value.is_null() {
            return Err("tried to decode NULL as PgJson".into());
        }

        let json_data = match value.format() {
            sqlx::postgres::PgValueFormat::Binary => {
                // This indicates that we are reading a JSONB column.
                let bytes = value.as_bytes()?;
                if bytes.is_empty() {
                    return Err("empty binary JSON data".into());
                }

                // JSONB stores a version byte at the beginning of the string,
                // which is currently 1.
                if bytes[0] == 1 {
                    std::str::from_utf8(&bytes[1..])?
                } else {
                    std::str::from_utf8(bytes)?
                }
            }
            sqlx::postgres::PgValueFormat::Text => value.as_str()?,
        };

        let result: T = serde_json::from_str(json_data)
            .map_err(|e| format!("JSON deserialization failed: {}", e))?;
        Ok(PgJson(result))
    }
}

#[cfg(feature = "sqlx")]
impl<T> sqlx::Type<sqlx::Postgres> for PgJson<T> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <serde_json::Value as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        use sqlx::TypeInfo;
        *ty == Self::type_info()
            || ty.name() == "JSON"
            || ty.name() == "JSONB"
            || ty.name() == "json"
            || ty.name() == "jsonb"
    }
}

#[cfg(feature = "sqlx")]
impl<'q, T> sqlx::Encode<'q, sqlx::Postgres> for PgJson<T>
where
    T: serde::Serialize,
{
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        match serde_json::to_value(&self.0) {
            Ok(json_value) => {
                <serde_json::Value as sqlx::Encode<sqlx::Postgres>>::encode(
                    json_value, buf,
                )
            }
            Err(_) => Err(Box::new(sqlx::Error::Encode("null value".into()))),
        }
    }
}
