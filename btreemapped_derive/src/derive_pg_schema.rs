use crate::derive_btreemapped::BTreeMappedArgs;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

pub fn derive_pg_schema(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the struct name
    let struct_name = input.ident;

    // Extract the btreemap attribute
    let btreemap_attr = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("btreemap"))
        .expect("Expected a #[btreemap(...)] attribute");

    // Parse the index fields from the attribute
    let args = btreemap_attr.parse_args::<BTreeMappedArgs>().unwrap();
    if args.index.is_empty() {
        panic!("Expected at least one index field");
    }
    let index_fields_names = args.index;

    // Ensure the input is a struct with named fields
    let fields = match input.data {
        Data::Struct(ref data_struct) => match &data_struct.fields {
            Fields::Named(ref fields_named) => &fields_named.named,
            _ => panic!("PgSchema can only be derived for structs with named fields"),
        },
        _ => panic!("PgSchema can only be derived for structs"),
    };

    let mut all_fields_idents = vec![];
    let mut all_fields_names = vec![];
    let mut all_fields_pg_types = vec![];

    for field in fields {
        if field.attrs.iter().any(|attr| attr.path().is_ident("skip")) {
            continue;
        }
        let name = field.ident.clone().expect("Expected named fields");
        all_fields_idents.push(name.clone());
        all_fields_names.push(name.to_string());
        if let Some(ty) = field
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("pg_type"))
            .map(|attr| attr.parse_args::<Type>().unwrap())
        {
            all_fields_pg_types.push(ty.clone());
        } else {
            panic!("Expected a #[pg_type(...)] attribute for field: {}", name);
        }
    }

    let num_fields = all_fields_idents.len();

    let (impl_generics, ty_generics, where_generics) = input.generics.split_for_impl();

    // Generate the impl block for BTreeMapped
    let impl_block = quote! {
        impl #impl_generics PgSchema for #struct_name #ty_generics #where_generics {
            fn column_names() -> impl ExactSizeIterator<Item = &'static str> {
                [
                    #(#all_fields_names),*
                ].into_iter()
            }

            fn column_types() -> impl ExactSizeIterator<Item = postgres_types::Type> {
                [
                    #(#all_fields_pg_types),*
                ].into_iter()
            }

            fn primary_key_column_names() -> Option<impl ExactSizeIterator<Item = &'static str>> {
                Some(
                    [
                        #(#index_fields_names),*
                    ].into_iter()
                )
            }

            fn columns_to_sql(&self) -> impl ExactSizeIterator<Item = &(dyn postgres_types::ToSql + Sync)> {
                let values: [&(dyn postgres_types::ToSql + Sync); #num_fields] = [
                    #(&self.#all_fields_idents),*
                ];
                values.into_iter()
            }
        }
    };

    // Combine everything
    let expanded = quote! {
        #impl_block
    };

    // Convert into a TokenStream and return
    TokenStream::from(expanded)
}
