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
