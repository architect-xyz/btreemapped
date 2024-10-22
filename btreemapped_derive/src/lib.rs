use proc_macro::TokenStream;

mod derive_btreemapped;
mod impl_for_range;

#[proc_macro_derive(BTreeMapped, attributes(btreemap, try_from, parse))]
pub fn derive_btreemapped(input: TokenStream) -> TokenStream {
    derive_btreemapped::derive_btreemapped(input)
}

#[proc_macro]
pub fn impl_for_range(input: TokenStream) -> TokenStream {
    impl_for_range::impl_for_range(input)
}
