use std::path::PathBuf;

use hf_hub::api::sync::ApiError;

use crate::error::FaultSource;

#[derive(Debug, thiserror::Error)]
#[error("Ошибка при генерации векторных встраиваний: {inner}")]
pub struct Error {
    pub inner: Box<ErrorKind>,
}

impl<I: Into<ErrorKind>> From<I> for Error {
    fn from(value: I) -> Self {
        Self { inner: Box::new(value.into()) }
    }
}

impl Error {
    pub fn fault(&self) -> FaultSource {
        match &*self.inner {
            ErrorKind::NewEmbedderError(inner) => inner.fault,
            ErrorKind::EmbedError(inner) => inner.fault,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error(transparent)]
    NewEmbedderError(#[from] NewEmbedderError),
    #[error(transparent)]
    EmbedError(#[from] EmbedError),
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct EmbedError {
    pub kind: EmbedErrorKind,
    pub fault: FaultSource,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedErrorKind {
    #[error("не удалось выполнить токенизацию: {0}")]
    Tokenize(Box<dyn std::error::Error + Send + Sync>),
    #[error("неожиданная форма тензора: {0}")]
    TensorShape(candle_core::Error),
    #[error("неожиданное значение тензора: {0}")]
    TensorValue(candle_core::Error),
    #[error("не удалось запустить модель: {0}")]
    ModelForward(candle_core::Error),
    #[error("попытка вставить следующий текст в конфигурацию, где вставки должны быть предоставлены пользователем: {0:?}")]
    ManualEmbed(String),
    #[error("модель не найдена. {0:?}")]
    OllamaModelNotFoundError(Option<String>),
    #[error("ошибка десериализации тела ответа в формате JSON: {0}")]
    RestResponseDeserialization(std::io::Error),
    #[error("компонент `{0}` не найден по пути `{1}` в ответе: `{2}`")]
    RestResponseMissingEmbeddings(String, String, String),
    #[error("неожиданный формат ответа на встраивание: {0}")]
    RestResponseFormat(serde_json::Error),
    #[error("ожидалось получить ответ, содержащий {0} встраиваний, а получили только {1}")]
    RestResponseEmbeddingCount(usize, usize),
    #[error("не удалось пройти аутентификацию на сервере встраивания: {0:?}")]
    RestUnauthorized(Option<String>),
    #[error("отправлено слишком много запросов на сервер встраивания: {0:?}")]
    RestTooManyRequests(Option<String>),
    #[error("отправил неверный запрос на сервер встраивания: {0:?}")]
    RestBadRequest(Option<String>),
    #[error("получен внутреннюю ошибку от сервера встраивания: {0:?}")]
    RestInternalServerError(u16, Option<String>),
    #[error("получен HTTP {0} от сервера встраивания: {0:?}")]
    RestOtherStatusCode(u16, Option<String>),
    #[error("не удалось связаться с сервером встраивания: {0}")]
    RestNetwork(ureq::Transport),
    #[error("ожидалось, что '{}' будет объектом в запросе '{0}'", .1.join("."))]
    RestNotAnObject(serde_json::Value, Vec<String>),
    #[error("при встраивании токенов ожидал размерности встраивания `{0}`, а было получено размерности `{1}`.")]
    OpenAiUnexpectedDimension(usize, usize),
    #[error("не было получено встраивание")]
    MissingEmbedding,
}

impl EmbedError {
    pub fn tokenize(inner: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self { kind: EmbedErrorKind::Tokenize(inner), fault: FaultSource::Runtime }
    }

    pub fn tensor_shape(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::TensorShape(inner), fault: FaultSource::Bug }
    }

    pub fn tensor_value(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::TensorValue(inner), fault: FaultSource::Bug }
    }

    pub fn model_forward(inner: candle_core::Error) -> Self {
        Self { kind: EmbedErrorKind::ModelForward(inner), fault: FaultSource::Runtime }
    }

    pub(crate) fn embed_on_manual_embedder(texts: String) -> EmbedError {
        Self { kind: EmbedErrorKind::ManualEmbed(texts), fault: FaultSource::User }
    }

    pub(crate) fn ollama_model_not_found(inner: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::OllamaModelNotFoundError(inner), fault: FaultSource::User }
    }

    pub(crate) fn rest_response_deserialization(error: std::io::Error) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestResponseDeserialization(error),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_response_missing_embeddings<S: AsRef<str>>(
        response: serde_json::Value,
        component: &str,
        response_field: &[S],
    ) -> EmbedError {
        let response_field: Vec<&str> = response_field.iter().map(AsRef::as_ref).collect();
        let response_field = response_field.join(".");

        Self {
            kind: EmbedErrorKind::RestResponseMissingEmbeddings(
                component.to_owned(),
                response_field,
                serde_json::to_string_pretty(&response).unwrap_or_default(),
            ),
            fault: FaultSource::Undecided,
        }
    }

    pub(crate) fn rest_response_format(error: serde_json::Error) -> EmbedError {
        Self { kind: EmbedErrorKind::RestResponseFormat(error), fault: FaultSource::Undecided }
    }

    pub(crate) fn rest_response_embedding_count(expected: usize, got: usize) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestResponseEmbeddingCount(expected, got),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_unauthorized(error_response: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::RestUnauthorized(error_response), fault: FaultSource::User }
    }

    pub(crate) fn rest_too_many_requests(error_response: Option<String>) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestTooManyRequests(error_response),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_bad_request(error_response: Option<String>) -> EmbedError {
        Self { kind: EmbedErrorKind::RestBadRequest(error_response), fault: FaultSource::User }
    }

    pub(crate) fn rest_internal_server_error(
        code: u16,
        error_response: Option<String>,
    ) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestInternalServerError(code, error_response),
            fault: FaultSource::Runtime,
        }
    }

    pub(crate) fn rest_other_status_code(code: u16, error_response: Option<String>) -> EmbedError {
        Self {
            kind: EmbedErrorKind::RestOtherStatusCode(code, error_response),
            fault: FaultSource::Undecided,
        }
    }

    pub(crate) fn rest_network(transport: ureq::Transport) -> EmbedError {
        Self { kind: EmbedErrorKind::RestNetwork(transport), fault: FaultSource::Runtime }
    }

    pub(crate) fn rest_not_an_object(
        query: serde_json::Value,
        input_path: Vec<String>,
    ) -> EmbedError {
        Self { kind: EmbedErrorKind::RestNotAnObject(query, input_path), fault: FaultSource::User }
    }

    pub(crate) fn openai_unexpected_dimension(expected: usize, got: usize) -> EmbedError {
        Self {
            kind: EmbedErrorKind::OpenAiUnexpectedDimension(expected, got),
            fault: FaultSource::Runtime,
        }
    }
    pub(crate) fn missing_embedding() -> EmbedError {
        Self { kind: EmbedErrorKind::MissingEmbedding, fault: FaultSource::Undecided }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{fault}: {kind}")]
pub struct NewEmbedderError {
    pub kind: NewEmbedderErrorKind,
    pub fault: FaultSource,
}

impl NewEmbedderError {
    pub fn open_config(config_filename: PathBuf, inner: std::io::Error) -> NewEmbedderError {
        let open_config = OpenConfig { filename: config_filename, inner };

        Self { kind: NewEmbedderErrorKind::OpenConfig(open_config), fault: FaultSource::Runtime }
    }

    pub fn deserialize_config(
        config: String,
        config_filename: PathBuf,
        inner: serde_json::Error,
    ) -> NewEmbedderError {
        let deserialize_config = DeserializeConfig { config, filename: config_filename, inner };
        Self {
            kind: NewEmbedderErrorKind::DeserializeConfig(deserialize_config),
            fault: FaultSource::Runtime,
        }
    }

    pub fn open_tokenizer(
        tokenizer_filename: PathBuf,
        inner: Box<dyn std::error::Error + Send + Sync>,
    ) -> NewEmbedderError {
        let open_tokenizer = OpenTokenizer { filename: tokenizer_filename, inner };
        Self {
            kind: NewEmbedderErrorKind::OpenTokenizer(open_tokenizer),
            fault: FaultSource::Runtime,
        }
    }

    pub fn new_api_fail(inner: ApiError) -> Self {
        Self { kind: NewEmbedderErrorKind::NewApiFail(inner), fault: FaultSource::Bug }
    }

    pub fn api_get(inner: ApiError) -> Self {
        Self { kind: NewEmbedderErrorKind::ApiGet(inner), fault: FaultSource::Undecided }
    }

    pub fn pytorch_weight(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::PytorchWeight(inner), fault: FaultSource::Runtime }
    }

    pub fn safetensor_weight(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::PytorchWeight(inner), fault: FaultSource::Runtime }
    }

    pub fn load_model(inner: candle_core::Error) -> Self {
        Self { kind: NewEmbedderErrorKind::LoadModel(inner), fault: FaultSource::Runtime }
    }

    pub fn could_not_determine_dimension(inner: EmbedError) -> NewEmbedderError {
        Self {
            kind: NewEmbedderErrorKind::CouldNotDetermineDimension(inner),
            fault: FaultSource::Runtime,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("не удалось открыть конфигурацию по адресу {filename:?}: {inner}")]
pub struct OpenConfig {
    pub filename: PathBuf,
    pub inner: std::io::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("cНе удается десериализовать конфигурацию {filename}: {inner}. Конфигурация :\n{config}")]
pub struct DeserializeConfig {
    pub config: String,
    pub filename: PathBuf,
    pub inner: serde_json::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("не удалось открыть токенизатор по адресу {filename}: {inner}")]
pub struct OpenTokenizer {
    pub filename: PathBuf,
    #[source]
    pub inner: Box<dyn std::error::Error + Send + Sync>,
}

#[derive(Debug, thiserror::Error)]
pub enum NewEmbedderErrorKind {
    // hf
    #[error(transparent)]
    OpenConfig(OpenConfig),
    #[error(transparent)]
    DeserializeConfig(DeserializeConfig),
    #[error(transparent)]
    OpenTokenizer(OpenTokenizer),
    #[error("не удалось создать весовые коэффициенты из весов Pytorch: {0}")]
    PytorchWeight(candle_core::Error),
    #[error("не удалось создать весовые коэффициенты из весов Safetensor: {0}")]
    SafetensorWeight(candle_core::Error),
    #[error("не удалось породить клиента HG_HUB API: {0}")]
    NewApiFail(ApiError),
    #[error("получение файла с HG_HUB не удалось: {0}")]
    ApiGet(ApiError),
    #[error("не удалось определить размеры модели: тестовое встраивание не удалось с {0}")]
    CouldNotDetermineDimension(EmbedError),
    #[error("загрузка модели не удалась: {0}")]
    LoadModel(candle_core::Error),
}
