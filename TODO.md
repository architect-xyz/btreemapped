Last 4 quick items

- the canonical method to replicate Decimal is to do try_from(PgNumeric)--make a note of this 
- use the same proc-macro-crate trick for LIndex
- pipe through conversion issues thru replicate (parse_row, etc.)
- remove Cell stuff from etl to make upstream easier
- fix integration test: use sqlx which will bypass the type issue...this doesn't fix the issue for tokio-postgres users though 

In:

    #[pg_type(Type::NUMERIC)]
    #[try_from(PgNumeric)]
    pub funding_rate_cap_lower_pct: Option<Decimal>,

btreemapped should understand pg_type(NUMERIC) and always use PgNumeric

then, what does this do?

    #[pg_type(Type::TEXT)]
    pub funding_rate_cap_lower_pct: Option<Decimal>,

should it be legal?


I think the republishing aspect of BTreeMap (and having an independent sequence number, etc) is too much sauce.
We should get rid of that and make it simpler.
Most clients only care (a) when the initial sync is ready and (b) how far behind the replication is