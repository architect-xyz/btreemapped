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
            .cloned()
            .collect();
        Ok(LIndexType { ident, args })
    }
}

pub fn impl_for_range(input: TokenStream) -> TokenStream {
    let LIndexType {
        ident: lindex_ident,
        args: lindex_args,
    } = parse_macro_input!(input as LIndexType);
    // the original LIndexN<..> type
    let lindex_ty = quote! { #lindex_ident<#(#lindex_args),*> };
    let mut range_methods = vec![];
    // the first 1..=N-1 range methods
    for i in 0..lindex_args.len() {
        // suppose arity N = 4
        // then our index type is LIndex4<I0, I1, I2, I3>
        // for_range3 should take:
        //   - 2 fixed parameters (convertible to I0, I1)
        //   - a range parameter (with endpoints convertible to I2)
        // and the BTreeMap range call would be over
        //   - exactly i0, i1 fixed
        //   - range over i2,
        //   - bounds on i3 chosen so that the fixed prefix and the
        //     inclusivity of the i2 endpoints are respected
        let mut fixed_generics = vec![];
        let mut fixed_params = vec![];
        let mut fixed_conversions = vec![];
        #[allow(clippy::needless_range_loop)]
        for j in 0..i {
            let param = format_ident!("x{}", j);
            let generic = format_ident!("X{}", j);
            let ty = lindex_args[j].clone();
            fixed_generics.push(generic.clone());
            fixed_params.push(quote! { #param: #generic, });
            fixed_conversions.push(quote! { let #param: #ty = #param.into(); });
        }
        let fixed_generic_bounds = fixed_generics
            .iter()
            .zip(lindex_args.iter())
            .map(|(generic, ty)| quote! { #generic: Into<#ty>, })
            .collect::<Vec<_>>();
        let range_param = lindex_args[i].clone();
        let for_range = format_ident!("for_range{}", i + 1);
        // Bind lists for the six (start/end x included/excluded/unbounded)
        // bound keys.  The trailing (j > i) sentinels are what make the
        // bounds correct: an excluded start must skip every key sharing
        // the excluded i-th element (so trailing Infinity), an excluded
        // end must stop before the first such key (trailing NegInfinity),
        // and unbounded ends clamp to the fixed prefix instead of the
        // whole map.
        let mut start_incl = vec![];
        let mut start_excl = vec![];
        let mut start_unb = vec![];
        let mut end_incl = vec![];
        let mut end_excl = vec![];
        let mut end_unb = vec![];
        for j in 0..lindex_args.len() {
            if j < i {
                let param = format_ident!("x{}", j);
                let tt = quote! { LValue::Exact(#param.clone()) };
                start_incl.push(tt.clone());
                start_excl.push(tt.clone());
                start_unb.push(tt.clone());
                end_incl.push(tt.clone());
                end_excl.push(tt.clone());
                end_unb.push(tt);
            } else if j == i {
                let exact = quote! { LValue::Exact(x) };
                start_incl.push(exact.clone());
                start_excl.push(exact.clone());
                start_unb.push(quote! { LValue::NegInfinity });
                end_incl.push(exact.clone());
                end_excl.push(exact);
                end_unb.push(quote! { LValue::Infinity });
            } else {
                // j > i
                start_incl.push(quote! { LValue::NegInfinity });
                start_excl.push(quote! { LValue::Infinity });
                start_unb.push(quote! { LValue::NegInfinity });
                end_incl.push(quote! { LValue::Infinity });
                end_excl.push(quote! { LValue::NegInfinity });
                end_unb.push(quote! { LValue::Infinity });
            }
        }
        range_methods.push(quote! {
            pub fn #for_range<#(#fixed_generics,)* R, F>(
                &self,
                #(#fixed_params)*
                range: R,
                mut f: F,
            )
            where
                #(#fixed_generic_bounds)*
                R: IntoBounds<#range_param>,
                F: FnMut(&T),
            {
                #(#fixed_conversions)*
                let (rstart, rend) = range.into_bounds();
                let start: std::ops::Bound<#lindex_ty> = match rstart {
                    std::ops::Bound::Included(x) => {
                        std::ops::Bound::Included(#lindex_ident(#(#start_incl,)*))
                    }
                    std::ops::Bound::Excluded(x) => {
                        std::ops::Bound::Excluded(#lindex_ident(#(#start_excl,)*))
                    }
                    std::ops::Bound::Unbounded => {
                        std::ops::Bound::Included(#lindex_ident(#(#start_unb,)*))
                    }
                };
                let end: std::ops::Bound<#lindex_ty> = match rend {
                    std::ops::Bound::Included(x) => {
                        std::ops::Bound::Included(#lindex_ident(#(#end_incl,)*))
                    }
                    std::ops::Bound::Excluded(x) => {
                        std::ops::Bound::Excluded(#lindex_ident(#(#end_excl,)*))
                    }
                    std::ops::Bound::Unbounded => {
                        std::ops::Bound::Included(#lindex_ident(#(#end_unb,)*))
                    }
                };
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
