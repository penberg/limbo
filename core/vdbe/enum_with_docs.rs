#[macro_export]
macro_rules! enum_with_docs {

    (
        $enum_name: ident {
        $(  #[description = $desc: literal]
            $variant_name: ident $({
                $(#[$label: ident])? $($var_name:ident : $var_type: ty $(,)?)*
            })?
        ),*
    }
    ) => {
        #[derive(Debug)]
        pub enum $enum_name {
            $(
                $variant_name $({
                    $($var_name: $var_type, )*
                })?,
            )*
        }
        impl $enum_name {
            fn get_description(&self) -> &str {
                match &self {
                    $(
                        $enum_name::$variant_name $({ $($var_name: _), * })? => {
                            $desc
                        }
                    )*
                }
            }
        }
    };
}
