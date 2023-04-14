#[async_trait::async_trait]
pub trait AsyncMutex {
    type Inner;
    type Guard<'a>: std::ops::DerefMut<Target = Self::Inner>
    where
        Self: 'a,
        Self::Inner: 'a;

    fn new(inner: Self::Inner) -> Self;

    async fn lock<'a>(&'a self) -> Self::Guard<'a>;
}

#[async_trait::async_trait]
impl<T: Send> AsyncMutex for std::sync::Mutex<T> {
    type Inner = T;
    type Guard<'a> = std::sync::MutexGuard<'a, T> where T: 'a;

    fn new(inner: Self::Inner) -> Self {
        Self::new(inner)
    }

    async fn lock<'a>(&'a self) -> Self::Guard<'a> {
        self.lock().unwrap()
    }
}

#[cfg(feature = "tokio")]
mod tokio_mutex {
    #[async_trait::async_trait]
    impl<T: Send> super::AsyncMutex for tokio::sync::Mutex<T> {
        type Inner = T;
        type Guard<'a> = tokio::sync::MutexGuard<'a, T> where T: 'a;

        fn new(inner: Self::Inner) -> Self {
            Self::new(inner)
        }

        async fn lock<'a>(&'a self) -> Self::Guard<'a> {
            self.lock().await
        }
    }
}
