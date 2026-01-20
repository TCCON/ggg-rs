#[derive(Debug, thiserror::Error)]
pub(crate) enum GggConcatError {
    #[error("Use error: {0}")]
    UseError(String),
    #[error("{0}")]
    Context(String),
}

impl GggConcatError {
    pub(crate) fn use_error<S: ToString>(msg: S) -> Self {
        Self::UseError(msg.to_string())
    }

    pub(crate) fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }
}
