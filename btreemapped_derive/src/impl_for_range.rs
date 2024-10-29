use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::Parse, parse_macro_input, AngleBracketedGenericArguments, GenericArgument,
    Ident, Type,
};

struct LIndexType {
    ident: Ident,
    args: Vec<Ident>,
}

impl Parse for LIndexType {
    // CR alee: some AI slop but it works for the narrow case
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let ident = input.parse()?;
        let bracketed: AngleBracketedGenericArguments = input.parse()?;
        let args: Vec<_> = bracketed
            .args
            .iter()
            .filter_map(|arg| {
                if let GenericArgument::Type(Type::Path(tp)) = arg {
                    Some(&tp.path.segments.last().unwrap().ident)
                } else {
                    None
                }
            })
            .map(|ident| ident.clone())
            .collect();
        Ok(LIndexType { ident, args })
    }
}

pub fn impl_for_range(input: TokenStream) -> TokenStream {
    let LIndexType { ident: lindex_ident, args: lindex_args } =
        parse_macro_input!(input as LIndexType);
    // the original LIndexN<..> type
    let lindex_ty = quote! { #lindex_ident<#(#lindex_args),*> };
    let mut range_methods = vec![];
    // the first 1..=N-1 range methods
    for i in 0..lindex_args.len() {
        // suppose arity N = 4
        // then our index type is LIndex4<I0, I1, I2, I3>
        // for_range3 should take:
        //   - 2 fixed parameters (I0, I1)
        //   - a range parameter (I2)
        // and the BTreeMap range call would be over
        //   - exactly i0, i1 fixed
        //   - range over i2,
        //   - open bounds on i3
        let mut fixed_params = vec![];
        for j in 0..i {
            let param = format_ident!("x{}", j);
            let ty = lindex_args[j].clone();
            fixed_params.push(quote! { #param: #ty, });
        }
        let range_param = lindex_args[i].clone();
        let for_range = format_ident!("for_range{}", i + 1);
        let mut start_bound_bind = vec![];
        let mut end_bound_bind = vec![];
        for j in 0..lindex_args.len() {
            if j < i {
                let param = format_ident!("x{}", j);
                let tt = quote! {
                    LValue::Exact(#param.clone())
                };
                start_bound_bind.push(tt.clone());
                end_bound_bind.push(tt);
            } else if j == i {
                // x-binding comes from range.{start, end}_bound.map(|x| ...)
                let tt = quote! {
                    LValue::Exact(x.clone())
                };
                start_bound_bind.push(tt.clone());
                end_bound_bind.push(tt);
            } else {
                // j > i
                start_bound_bind.push(quote! { LValue::NegInfinity });
                end_bound_bind.push(quote! { LValue::Infinity });
            }
        }
        range_methods.push(quote! {
            pub fn #for_range<R, F>(&self, #(#fixed_params)* range: R, mut f: F)
            where
                R: std::ops::RangeBounds<#range_param>,
                F: FnMut(&T),
            {
                let start: std::ops::Bound<#lindex_ty> = range
                    .start_bound()
                    .map(|x| #lindex_ident(#(#start_bound_bind,)*));
                let end: std::ops::Bound<#lindex_ty> = range
                    .end_bound()
                    .map(|x| #lindex_ident(#(#end_bound_bind,)*));
                let replica = self.replica.read();
                for (_i, t) in replica.range((start, end)) {
                    f(t);
                }
            }
        });
    }
    let for_index_arity = lindex_args.len();
    let expanded = quote! {
        impl<T, #(#lindex_args),*> BTreeMapReplica<T, #for_index_arity>
        where
            T: BTreeMapped<#for_index_arity, LIndex = #lindex_ty>,
            #(#lindex_args: Clone + Ord + 'static,)*
        {
            #(#range_methods)*
        }
    };
    TokenStream::from(expanded)
}
