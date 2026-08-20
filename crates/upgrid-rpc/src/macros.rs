/// Defines a typed RPC service, client, server adapter, and wire enums.
#[macro_export]
macro_rules! service {
    (
        $visibility:vis service {
            trait $service:ident;
            client $client:ident;
            server $server:ident;
            request $request:ident;
            response $response:ident;
            $(
                $variant:ident => $method:ident(
                    $($argument:ident: $argument_type:ty),* $(,)?
                ) -> $output:ty;
            )+
        }
    ) => {
        #[allow(async_fn_in_trait)]
        $visibility trait $service: Clone + 'static {
            $(
                async fn $method(
                    self,
                    context: $crate::Context,
                    $($argument: $argument_type),*
                ) -> $output;
            )+
        }

        #[derive(Debug, $crate::__serde::Deserialize, $crate::__serde::Serialize)]
        $visibility enum $request {
            $(
                $variant { $($argument: $argument_type),* },
            )+
        }

        impl $crate::RequestName for $request {
            fn name(&self) -> &'static str {
                match self {
                    $(Self::$variant { .. } => stringify!($method),)+
                }
            }
        }

        #[derive(Debug, $crate::__serde::Deserialize, $crate::__serde::Serialize)]
        $visibility enum $response {
            $($variant($output),)+
        }

        impl $crate::RequestName for $response {
            fn name(&self) -> &'static str {
                match self {
                    $(Self::$variant(_) => stringify!($method),)+
                }
            }
        }

        #[derive(Clone)]
        $visibility struct $client {
            inner: $crate::client::Client<
                $request,
                Result<$response, $crate::server::ServerError>,
            >,
        }

        impl $client {
            $visibility fn new<T>(
                transport: T,
            ) -> (
                Self,
                $crate::client::Dispatch<
                    $request,
                    Result<$response, $crate::server::ServerError>,
                    T,
                >,
            )
            where
                T: $crate::Transport<
                    $crate::ClientMessage<$request>,
                    $crate::Response<Result<$response, $crate::server::ServerError>>,
                >,
            {
                let (inner, dispatch) = $crate::client::new(transport);
                (Self { inner }, dispatch)
            }

            $(
                $visibility async fn $method(
                    &self,
                    context: $crate::Context,
                    $($argument: $argument_type),*
                ) -> Result<$output, $crate::CallError> {
                    let response = self
                        .inner
                        .call(context, $request::$variant { $($argument),* })
                        .await?;
                    let response = match response {
                        Ok(response) => response,
                        Err($crate::server::ServerError::DeadlineExceeded) => {
                            return Err($crate::CallError::DeadlineExceeded);
                        }
                    };
                    match response {
                        $response::$variant(response) => Ok(response),
                        other => Err($crate::CallError::UnexpectedResponse {
                            expected: stringify!($method),
                            received: $crate::RequestName::name(&other),
                        }),
                    }
                }
            )+
        }

        #[derive(Clone)]
        $visibility struct $server<S> {
            service: S,
        }

        impl<S> $server<S> {
            $visibility fn new(service: S) -> Self {
                Self { service }
            }
        }

        impl<S> $crate::server::Service for $server<S>
        where
            S: $service,
        {
            type Request = $request;
            type Response = $response;

            async fn serve(
                &self,
                context: $crate::Context,
                request: Self::Request,
            ) -> Self::Response {
                match request {
                    $(
                        $request::$variant { $($argument),* } => {
                            $response::$variant(
                                self.service.clone().$method(context, $($argument),*).await,
                            )
                        }
                    )+
                }
            }
        }
    };
}
