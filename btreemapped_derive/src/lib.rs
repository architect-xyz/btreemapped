use proc_macro::TokenStream;

mod derive_btreemapped;
mod derive_pg_schema;
mod impl_for_range;

#[proc_macro_derive(BTreeMapped, attributes(btreemap, parse, pg_enum, try_from))]
pub fn derive_btreemapped(input: TokenStream) -> TokenStream {
    derive_btreemapped::derive_btreemapped(input)
}

#[proc_macro_derive(PgSchema, attributes(btreemap, pg_type, try_from))]
pub fn derive_pg_schema(input: TokenStream) -> TokenStream {
    derive_pg_schema::derive_pg_schema(input)
}

#[proc_macro]
pub fn impl_for_range(input: TokenStream) -> TokenStream {
    impl_for_range::impl_for_range(input)
}
