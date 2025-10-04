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
