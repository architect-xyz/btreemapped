use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::{HashMap, HashSet};
use syn::{
    parse::{Parse, ParseStream, Result},
    parse_macro_input, Data, DeriveInput, Fields, Ident, LitStr, Token, Type,
};

pub fn derive_btreemapped(input: TokenStream) -> TokenStream {
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
        // CR alee: no index fields should mean the same as all fields are index fields,
        // in field definition order.  I believe this matches the behavior of postgres
        // replication
    }
    let index_fields_names = args.index;

    // Ensure the input is a struct with named fields
    let fields = match input.data {
        Data::Struct(ref data_struct) => match &data_struct.fields {
            Fields::Named(ref fields_named) => &fields_named.named,
            _ => panic!("BTreeMapped can only be derived for structs with named fields"),
        },
        _ => panic!("BTreeMapped can only be derived for structs"),
    };

    // Collect field names and types, sift into index/unindexed
    // Indexed fields should be in the order of the spec
    let mut index_fields_by_name: HashMap<String, (Ident, Type)> = HashMap::new();
    let mut index_fields = vec![];
    let mut index_field_types = vec![]; // just the types in order, for convenience
    let mut lindex_field_types = vec![]; // same as index_field_types, except String => str
    let mut unindexed_fields = vec![];
    let mut unindexed_field_types = vec![]; // just the types in order, for convenience
    let mut all_fields = vec![]; // all fields, for convenience
    let mut field_try_from: HashMap<Ident, Type> = HashMap::new();
    let mut field_parse: HashSet<Ident> = HashSet::new();

    for field in fields {
        let name = field.ident.clone().expect("Expected named fields");
        let ty = field.ty.clone();
        if let Some(try_from_ty) = field
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("try_from"))
            .map(|attr| attr.parse_args::<Type>().unwrap())
        {
            field_try_from.insert(name.clone(), try_from_ty);
        } else if field.attrs.iter().any(|attr| attr.path().is_ident("parse")) {
            field_parse.insert(name.clone());
        }
        all_fields.push((name.clone(), ty.clone()));
        if index_fields_names.contains(&name.to_string()) {
            index_fields_by_name.insert(name.to_string(), (name, ty));
        } else {
            unindexed_field_types.push(ty.clone());
            unindexed_fields.push((name, ty));
        }
    }

    for name in index_fields_names.iter() {
        let (ident, ty) = index_fields_by_name.remove(name).unwrap();
        index_fields.push((ident, ty.clone()));
        index_field_types.push(ty.clone());
        // if is_string_type(&ty) {
        //     lindex_field_types.push(Type::Verbatim(quote!(std::borrow::Cow<'static, str>)));
        // } else {
        lindex_field_types.push(ty);
        // }
    }

    // Generate the index tuple constructor for into_kv
    let index_tuple_ctor = if index_fields.len() == 1 {
        let (name, _) = &index_fields[0];
        quote! { (self.#name,) }
    } else {
        let names = index_fields.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
        quote! { (#(self.#names),*) }
    };

    // Generate the unindexed tuple constructor for into_kv
    let unindexed_tuple_ctor = if unindexed_fields.len() == 0 {
        quote! { () }
    } else if unindexed_fields.len() == 1 {
        let (name, _) = &unindexed_fields[0];
        quote! { (self.#name,) }
    } else {
        let names =
            unindexed_fields.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
        quote! { (#(self.#names),*) }
    };

    // Generate Ref struct name
    let ref_struct_name = format_ident!("{}Ref", struct_name);

    // Generate Ref struct fields with lifetime and type transformations
    let ref_fields = all_fields.iter().map(|(name, ty)| {
        let ref_ty = if is_string_type(ty) {
            quote! { &'a str }
        } else {
            quote! { &'a #ty }
        };
        quote! { pub #name: #ref_ty }
    });

    // Generate the Ref struct
    let ref_struct = quote! {
        #[derive(Debug, Clone, Serialize)]
        pub struct #ref_struct_name<'a> {
            #(#ref_fields),*
        }
    };

    // Determine the number of index fields to select appropriate LIndexN
    let index_len = index_fields.len();
    let lindex_ident = format_ident!("LIndex{}", index_len);
    let lindex_type = quote! { #lindex_ident<#(#lindex_field_types),*> };

    // Generate the Index type tuple
    let index_tuple = if index_fields.len() == 1 {
        let (_, ty) = index_fields[0].clone();
        quote! { (#ty,) }
    } else {
        quote! { (#(#index_field_types),*) }
    };

    // Generate Unindexed type tuple
    let unindexed_type = if unindexed_fields.len() == 0 {
        quote! { () }
    } else if unindexed_fields.len() == 1 {
        let (_, ty) = unindexed_fields[0].clone();
        quote! { (#ty,) }
    } else {
        quote! { (#(#unindexed_field_types),*) }
    };

    // Generate struct fields for kv_as_ref
    let mut kv_ref_fields = vec![];
    for (i, (name, _)) in index_fields.iter().enumerate() {
        let idx = syn::Index::from(i);
        kv_ref_fields.push(quote! { #name: index .#idx.exact()? });
    }
    for (i, (name, _)) in unindexed_fields.iter().enumerate() {
        let idx = syn::Index::from(i);
        kv_ref_fields.push(quote! { #name: &unindexed.#idx });
    }

    // Generate statements for parse_row
    let mut parse_row_var_decls = vec![];
    for (name, ty) in &index_fields {
        parse_row_var_decls.push(quote! { let mut #name: Option<#ty> = None; });
    }
    for (name, ty) in &unindexed_fields {
        parse_row_var_decls.push(quote! { let mut #name: Option<#ty> = None; });
    }

    let mut parse_row_match_arms = vec![];
    for (name, _) in &all_fields {
        let name_str = name.to_string();
        let conversion_arm = match field_try_from.get(name) {
            Some(try_from_ty) => {
                // use an intermediate type to convert from SQL
                quote! {
                    #name_str => {
                        let _i = TryInto::<#try_from_ty>::try_into(v).ok();
                        #name = _i.and_then(|i| i.try_into().ok());
                    }
                }
            }
            None => {
                if field_parse.contains(name) {
                    quote! {
                        #name_str => {
                            let _i = TryInto::<String>::try_into(v).ok();
                            #name = _i.and_then(|i| i.parse().ok());
                        }
                    }
                } else {
                    quote! { #name_str => { #name = v.try_into().ok(); } }
                }
            }
        };
        parse_row_match_arms.push(conversion_arm);
    }

    let mut unwrap_index_vars = vec![];
    for (name, _) in &index_fields {
        unwrap_index_vars.push(quote! { let #name = #name?; });
    }

    let mut unwrap_unindexed_vars = vec![];
    for (name, _) in &unindexed_fields {
        unwrap_unindexed_vars.push(quote! { let #name = #name?; });
    }

    let parse_row_index_ctor = if index_fields_names.len() == 1 {
        let (name, _) = &index_fields[0];
        quote! { (#name,) }
    } else {
        let names = index_fields.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
        quote! { (#(#names),*) }
    };

    let parse_row_unindexed_ctor = if unindexed_fields.len() == 0 {
        quote! { () }
    } else if unindexed_fields.len() == 1 {
        let (name, _) = &unindexed_fields[0];
        quote! { (#name,) }
    } else {
        let names =
            unindexed_fields.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>();
        quote! { (#(#names),*) }
    };

    let index_arity = index_fields.len();

    // Generate the impl block for BTreeMapped
    let impl_block = quote! {
        impl BTreeMapped<#index_arity> for #struct_name {
            type Ref<'a> = #ref_struct_name<'a>;
            type LIndex = #lindex_type;
            type Index = #index_tuple;
            type Unindexed = #unindexed_type;

            fn into_kv(self) -> (Self::Index, Self::Unindexed) {
                (#index_tuple_ctor, #unindexed_tuple_ctor)
            }

            fn kv_as_ref<'a>(
                index: &'a Self::LIndex,
                unindexed: &'a Self::Unindexed,
            ) -> Option<Self::Ref<'a>> {
                Some(#ref_struct_name {
                    #(#kv_ref_fields),*
                })
            }

            fn parse_row(
                schema: &pg_replicate::table::TableSchema,
                row: pg_replicate::conversions::table_row::TableRow,
            ) -> (Option<Self::Index>, Option<Self::Unindexed>) {
                #(#parse_row_var_decls;)*
                let mut n = 0;
                for v in row.values {
                    let col = &schema.column_schemas[n];
                    match col.name.as_ref() {
                        #(#parse_row_match_arms,)*
                        _ => {}
                    }
                    n += 1;
                }
                let maybe_index = || {
                    #(#unwrap_index_vars)*
                    Some(#parse_row_index_ctor)
                };
                let maybe_unindexed = || {
                    #(#unwrap_unindexed_vars)*
                    Some(#parse_row_unindexed_ctor)
                };
                (maybe_index(), maybe_unindexed())
            }
        }
    };

    // Combine everything
    let expanded = quote! {
        #ref_struct

        #impl_block
    };

    // Convert into a TokenStream and return
    TokenStream::from(expanded)
}

// CR alee: pretty sure this is unsound, let's see if it ends up being
// "good enough" for awhile
fn is_string_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            let segment = type_path.path.segments.last().unwrap();
            segment.ident == "String"
        }
        _ => false,
    }
}

/// Struct to parse the btreemap attribute arguments
struct BTreeMappedArgs {
    index: Vec<String>,
}

impl Parse for BTreeMappedArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        // Expect "index = ["field1", "field2", ...]"
        let mut index = Vec::new();

        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            if ident == "index" {
                let content;
                syn::bracketed!(content in input);
                while !content.is_empty() {
                    let lit: LitStr = content.parse()?;
                    index.push(lit.value());
                    if content.peek(Token![,]) {
                        content.parse::<Token![,]>()?;
                    } else {
                        break;
                    }
                }
            } else {
                return Err(syn::Error::new(ident.span(), "Unknown attribute key"));
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(BTreeMappedArgs { index })
    }
}
