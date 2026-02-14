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
    let lindex_ty = quote! { #lindex_ident<#(#lindex_args),*> };
    let for_index_arity = lindex_args.len();

    if for_index_arity == 1 {
        // Singleton: zero-alloc via Borrow<I0::Borrowed>.
        //
        // LBorrowable maps each index type to its borrowed form
        // (String → str, i64 → i64, etc.), resolving the ambiguity
        // between our Borrow<str> and std's blanket Borrow<Self>.
        let i0 = &lindex_args[0];
        let expanded = quote! {
            impl<T, #i0> BTreeMapReplica<T, 1>
            where
                T: BTreeMapped<1, LIndex = LIndex1<#i0>>,
                #i0: LBorrowable,
                LIndex1<#i0>: std::borrow::Borrow<<#i0 as LBorrowable>::Borrowed>,
            {
                pub fn for_range1<Q, R, F>(&self, range: R, mut f: F)
                where
                    Q: std::borrow::Borrow<<#i0 as LBorrowable>::Borrowed>,
                    R: std::ops::RangeBounds<Q>,
                    F: FnMut(&T),
                {
                    let start = range.start_bound().map(|x|
                        <Q as std::borrow::Borrow<<#i0 as LBorrowable>::Borrowed>>::borrow(x)
                    );
                    let end = range.end_bound().map(|x|
                        <Q as std::borrow::Borrow<<#i0 as LBorrowable>::Borrowed>>::borrow(x)
                    );
                    let replica = self.replica.read();
                    for (_, t) in replica.range::<<#i0 as LBorrowable>::Borrowed, _>((start, end)) {
                        f(t);
                    }
                }
            }
        };
        TokenStream::from(expanded)
    } else {
        // Composite: Into-based.  Each dimension accepts a generic Q
        // that converts via Into<I>.  Zero-alloc for Cow<'static, str>
        // with string literals (Into creates Cow::Borrowed) and for
        // all Copy types.
        let mut range_methods = vec![];
        for i in 0..lindex_args.len() {
            // suppose arity N = 4
            // then our index type is LIndex4<I0, I1, I2, I3>
            // for_range3 should take:
            //   - 2 fixed parameters (Q0 -> I0, Q1 -> I1)
            //   - a range parameter (Q2 -> I2)
            // and the BTreeMap range call would be over
            //   - exactly i0, i1 fixed (already converted)
            //   - range over i2 (converted in bound map)
            //   - open bounds on i3

            // Generic Q params for dims 0..=i
            let q_params: Vec<_> = (0..=i).map(|j| format_ident!("Q{}", j)).collect();

            // Fixed params: x0: Q0, x1: Q1, ...
            let mut fixed_params = vec![];
            for j in 0..i {
                let param = format_ident!("x{}", j);
                let q_ty = &q_params[j];
                fixed_params.push(quote! { #param: #q_ty, });
            }

            // Where predicates for Q types
            let mut q_where_preds = vec![];
            for j in 0..i {
                let q = &q_params[j];
                let i_ty = &lindex_args[j];
                q_where_preds.push(quote! { #q: Into<#i_ty>, });
            }
            let range_q = &q_params[i];
            let range_i = &lindex_args[i];
            q_where_preds.push(quote! { #range_q: Clone + Into<#range_i>, });

            let for_range = format_ident!("for_range{}", i + 1);

            // Convert fixed params: let x0: I0 = x0.into();
            let mut conversions = vec![];
            for j in 0..i {
                let param = format_ident!("x{}", j);
                let i_ty = &lindex_args[j];
                conversions.push(quote! { let #param: #i_ty = #param.into(); });
            }

            // Bound construction
            let mut start_bound_bind = vec![];
            let mut end_bound_bind = vec![];
            for j in 0..lindex_args.len() {
                if j < i {
                    // Fixed dimension: already converted to I_j
                    let param = format_ident!("x{}", j);
                    let tt = quote! { LValue::Exact(#param.clone()) };
                    start_bound_bind.push(tt.clone());
                    end_bound_bind.push(tt);
                } else if j == i {
                    // Range dimension: x comes from range bound, convert Q -> I
                    let tt = quote! { LValue::Exact(x.clone().into()) };
                    start_bound_bind.push(tt.clone());
                    end_bound_bind.push(tt);
                } else {
                    // Open dimension: NegInfinity for start, Infinity for end
                    start_bound_bind.push(quote! { LValue::NegInfinity });
                    end_bound_bind.push(quote! { LValue::Infinity });
                }
            }

            range_methods.push(quote! {
                pub fn #for_range<#(#q_params,)* R, F>(&self, #(#fixed_params)* range: R, mut f: F)
                where
                    #(#q_where_preds)*
                    R: std::ops::RangeBounds<#range_q>,
                    F: FnMut(&T),
                {
                    #(#conversions)*
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
}
