use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::Write;
use std::{io, str};

use heed::{Error as HeedError, MdbError};
use rayon::ThreadPoolBuildError;
use serde_json::Value;
use thiserror::Error;

use crate::documents::{self, DocumentsBatchCursorError};
use crate::{CriterionError, DocumentId, FieldId, Object, SortError};
use crate::update::thread_pool_no_abort::PanicCatched;


#[derive(Error, Debug)]
pub enum Error {
    #[error("внутренняя: {0}.")]
    InternalError(#[from] InternalError),
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    UserError(#[from] UserError),
}

#[derive(Error, Debug)]
pub enum InternalError {
    #[error("{}", HeedError::DatabaseClosing)]
    DatabaseClosing,
    #[error("Отсутствует {} в базе данных {db_name}.", key.unwrap_or("key"))]
    DatabaseMissingEntry { db_name: &'static str, key: Option<&'static str> },
    #[error(transparent)]
    FieldIdMapMissingEntry(#[from] FieldIdMapMissingEntry),
    #[error("Отсутствует {key} для сопоставлении id поля.")]
    FieldIdMappingMissingEntry { key: FieldId },
    #[error(transparent)]
    Fst(#[from] fst::Error),
    #[error(transparent)]
    DocumentsError(#[from] documents::Error),
    #[error("Для grenad был указан неверный тип сжатия.")]
    GrenadInvalidCompressionType,
    #[error("Неверный файл grenad.")]
    GrenadInvalidFormatVersion,
    #[error("Неверное слияние при обработке {process}.")]
    IndexingMergingKeys { process: &'static str },
    #[error("{}", HeedError::InvalidDatabaseTyping)]
    InvalidDatabaseTyping,
    #[error(transparent)]
    RayonThreadPool(#[from] ThreadPoolBuildError),
    #[error(transparent)]
    PanicInThreadPool(#[from] PanicCatched),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Serialization(#[from] SerializationError),
    #[error(transparent)]
    Store(#[from] MdbError),
    #[error(transparent)]
    Utf8(#[from] str::Utf8Error),
    #[error("Процесс индексации был прерван.")]
    AbortedIndexation,
    #[error("Список подходящих слов содержит по крайней мере один недопустимый член.")]
    InvalidMatchingWords,
    #[error(transparent)]
    ArroyError(#[from] arroy::Error),
    #[error(transparent)]
    VectorEmbeddingError(#[from] crate::vector::Error),
}

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("{}", match .db_name {
        Some(name) => format!("Декодирование из базы данных {name} не удалось"),
        None => "decoding failed".to_string(),
    })]
    Decoding { db_name: Option<&'static str> },
    #[error("{}", match .db_name {
        Some(name) => format!("Кодирование в базу данных {name} не удалось"),
        None => "encoding failed".to_string(),
    })]
    Encoding { db_name: Option<&'static str> },
    #[error("число не является конечным")]
    InvalidNumberSerialization,
}

#[derive(Error, Debug)]
pub enum FieldIdMapMissingEntry {
    #[error("неизвестное id поля {field_id} из процесса {process}")]
    FieldId { field_id: FieldId, process: &'static str },
    #[error("неизвестное имя поля {field_name} из процесса {process}")]
    FieldName { field_name: String, process: &'static str },
}

#[derive(Error, Debug)]
pub enum UserError {
    #[error("Документ не может содержать более 65 535 полей.")]
    AttributeLimitReached,
    #[error(transparent)]
    CriterionError(#[from] CriterionError),
    #[error("Достигнуто максимальное количество документов.")]
    DocumentLimitReached,
    #[error("Идентификатор документа `{}` недействителен. Идентификатор документа должен быть либо целым числом, либо строкой", .document_id.to_string()
    )]
    InvalidDocumentId { document_id: Value },
    #[error("Неверное распределение фасетов, {}", format_invalid_filter_distribution(.invalid_facets_name, .valid_facets_name))]
    InvalidFacetsDistribution {
        invalid_facets_name: BTreeSet<String>,
        valid_facets_name: BTreeSet<String>,
    },
    #[error(transparent)]
    InvalidGeoField(#[from] GeoError),
    #[error("Недопустимые размеры вектора: ожидается: `{}`, найдено: `{}`.", .expected, .found)]
    InvalidVectorDimensions { expected: usize, found: usize },
    #[error("Поле `_vectors.{subfield}` в документе: `{document_id}` не является массивом. Ожидался массив значений с плавающей точкой или массив массивов значений с плавающей точкойй, но было получено `{value}`.")]
    InvalidVectorsType { document_id: Value, value: Value, subfield: String },
    #[error("Поле `_vectors` в документе: `{document_id}` не является объектом. Ожидался объект с ключом для каждого эмбеддера с предоставленными вручную векторами, но вместо этого было получено `{value}`")]
    InvalidVectorsMapType { document_id: Value, value: Value },
    #[error("{0}")]
    InvalidFilter(String),
    #[error("Неверный тип для фильтра: ожидается: {}, найдено: {1}.", .0.join(", "))]
    InvalidFilterExpression(&'static [&'static str], Value),
    #[error("Аттрибут `{}` не сортируемый. {}",
        .field,
        match .valid_fields.is_empty() {
            true => "Этот индекс не имеет настроенных сортируемых атрибутов.".to_string(),
            false => format!("Доступны следующие сортируемые атрибуты: `{}`.",
                    valid_fields.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", "),
                ),
        }
    )]
    InvalidSortableAttribute { field: String, valid_fields: BTreeSet<String>},
    #[error("Атрибут `{}` не поддается фасетному поиску. {}",
        .field,
        match .valid_fields.is_empty() {
            true => "Этот индекс не имеет настроенных атрибутов для фасетного поиска. Чтобы сделать его пригодным для поиска по фасетам, добавьте его в настройки индекса `filterableAttributes`.".to_string(),
            false => format!("Доступны следующие атрибуты для поиска по фасетам: `{}`. Чтобы сделать его пригодным для поиска по фасетам, добавьте его в настройки индекса `filterableAttributes`.",
                    valid_fields.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", "),
                ),
        }
    )]
    InvalidFacetSearchFacetName {
        field: String,
        valid_fields: BTreeSet<String>,
    },
    #[error("Атрибуй `{}` не является доступным для поиска. Доступными для поиска атрибутами являются: `{}`.",
        .field,
        .valid_fields.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", "),
    )]
    InvalidSearchableAttribute {
        field: String,
        valid_fields: BTreeSet<String>,
    },
    #[error("Уже открыта среда с другими опциями")]
    InvalidLmdbOpenOptions,
    #[error("Чтобы использовать параметр сортировки во время поиска, необходимо указать критерий `sort` в настройке rankingRules.")]
    SortRankingRuleMissing,
    #[error("Файл базы данных находится в некорректном состоянии.")]
    InvalidStoreFile,
    #[error("Достигнут максимальный размер базы данных.")]
    MaxDatabaseSizeReached,
    #[error("Документ не имеет атрибута `{}`: `{}`.", .primary_key, serde_json::to_string(.document).unwrap())]
    MissingDocumentId { primary_key: String, document: Object },
    #[error("В документе несколько атрибутов `{}`: `{}`.", .primary_key, serde_json::to_string(.document).unwrap())]
    TooManyDocumentIds { primary_key: String, document: Object },
    #[error("Вывод первичного ключа не удался, поскольку не было найдено ни одного поля, в названии которого содержалось бы `id`. Пожалуйста, укажите первичный ключ вручную с помощью параметра запроса `primaryKey`.")]
    NoPrimaryKeyCandidateFound,
    #[error("Вывод первичного ключа не удался, так как было обнаружено {} поля, имена которых заканчиваются на `id`: '{}' и '{}'. Пожалуйста, укажите первичный ключ вручную с помощью параметра запроса `primaryKey`.", .candidates.len(), .candidates.first().unwrap(), .candidates.get(1).unwrap())]
    MultiplePrimaryKeyCandidatesFound { candidates: Vec<String> },
    #[error("На устройстве не осталось свободного места.")]
    NoSpaceLeftOnDevice,
    #[error("Индекс уже имеет первичный ключ: `{0}`.")]
    PrimaryKeyCannotBeChanged(String),
    #[error(transparent)]
    SerdeJson(serde_json::Error),
    #[error(transparent)]
    SortError(#[from] SortError),
    #[error("Был использован неизвестный идентификатор документа: `{document_id}`.")]
    UnknownInternalDocumentId { document_id: DocumentId },
    #[error("Настройка `typoTolerance` недействительна. `twoTypos` должны быть больше или равны `oneTypo`.")]
    InvalidMinTypoWordLenSetting(u8, u8),
    #[error(transparent)]
    VectorEmbeddingError(#[from] crate::vector::Error),
    #[error(transparent)]
    MissingDocumentField(#[from] crate::prompt::error::RenderPromptError),
    #[error(transparent)]
    InvalidPrompt(#[from] crate::prompt::error::NewPromptError),
    #[error("`.embedders.{0}.documentTemplate`: Некорректный шаблон: {1}.")]
    InvalidPromptForEmbeddings(String, crate::prompt::error::NewPromptError),
    #[error("Слишком много генераторов векторных встраиваний. Найдено {0}, но ограничено 256.")]
    TooManyEmbedders(usize),
    #[error("Не найдет генератор векторных встраиваний с именем `{0}`.")]
    InvalidEmbedder(String),
    #[error("Не найдет текстовый анализатор с именем `{0}`.")]
    InvalidAnalyzer(String),
    #[error("Отсутствует текстовый анализатор по умолчанию.")]
    NoDefaultAnalyzer,
    #[error("Слишком много векторов для документа с идентификатором {0}: найдено {1}, но ограничено 256.")]
    TooManyVectors(String, usize),
    #[error("`.embedders.{embedder_name}`: Поле `{field}` недоступно для источника `{source_}` (Доступны для источника: {}). Доступные поля: {}",
        allowed_sources_for_field
         .iter()
         .map(|accepted| format!("`{}`", accepted))
         .collect::<Vec<String>>()
         .join(", "),
        allowed_fields_for_source
         .iter()
         .map(|accepted| format!("`{}`", accepted))
         .collect::<Vec<String>>()
         .join(", ")
    )]
    InvalidFieldForSource {
        embedder_name: String,
        source_: crate::vector::settings::EmbedderSource,
        field: &'static str,
        allowed_fields_for_source: &'static [&'static str],
        allowed_sources_for_field: &'static [crate::vector::settings::EmbedderSource],
    },
    #[error("`.embedders.{embedder_name}.model`: Недопустимая модель `{model}` для OpenAI. Поддерживаемые модели: {:?}", crate::vector::openai::EmbeddingModel::supported_models())]
    InvalidOpenAiModel { embedder_name: String, model: String },
    #[error("`.embedders.{embedder_name}`: Отсутствует поле `{field}` (Это поле обязательно для источника {source_})")]
    MissingFieldForSource {
        field: &'static str,
        source_: crate::vector::settings::EmbedderSource,
        embedder_name: String,
    },
    #[error("`.embedders.{embedder_name}.dimensions`: Модель `{model}` не поддерживает переопределение размеров {expected_dimensions}. Найдено {dimensions}")]
    InvalidOpenAiModelDimensions {
        embedder_name: String,
        model: &'static str,
        dimensions: usize,
        expected_dimensions: usize,
    },
    #[error("`.embedders.{embedder_name}.dimensions`: Модель `{model}` не поддерживает переопределение размеров на значение, превышающее {max_dimensions}. Найдено {dimensions}")]
    InvalidOpenAiModelDimensionsMax {
        embedder_name: String,
        model: &'static str,
        dimensions: usize,
        max_dimensions: usize,
    },
    #[error("`.embedders.{embedder_name}.dimensions`: не может быть нулевым")]
    InvalidSettingsDimensions { embedder_name: String },
    #[error("`.embedders.{embedder_name}.url`: Невозможно разобрать `{url}`: {inner_error}")]
    InvalidUrl { embedder_name: String, inner_error: url::ParseError, url: String },
}

impl From<crate::vector::Error> for Error {
    fn from(value: crate::vector::Error) -> Self {
        match value.fault() {
            FaultSource::User => Error::UserError(value.into()),
            FaultSource::Runtime => Error::InternalError(value.into()),
            FaultSource::Bug => Error::InternalError(value.into()),
            FaultSource::Undecided => Error::InternalError(value.into()),
        }
    }
}

impl From<arroy::Error> for Error {
    fn from(value: arroy::Error) -> Self {
        match value {
            arroy::Error::Heed(heed) => heed.into(),
            arroy::Error::Io(io) => io.into(),
            arroy::Error::InvalidVecDimension { expected, received } => {
                Error::UserError(UserError::InvalidVectorDimensions { expected, found: received })
            }
            arroy::Error::DatabaseFull
            | arroy::Error::InvalidItemAppend
            | arroy::Error::UnmatchingDistance { .. }
            | arroy::Error::MissingNode
            | arroy::Error::MissingMetadata => {
                Error::InternalError(InternalError::ArroyError(value))
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum GeoError {
    #[error("The `_geo` field in the document with the id: `{document_id}` is not an object. Was expecting an object with the `_geo.lat` and `_geo.lng` fields but instead got `{value}`.")]
    NotAnObject { document_id: Value, value: Value },
    #[error("The `_geo` field in the document with the id: `{document_id}` contains the following unexpected fields: `{value}`.")]
    UnexpectedExtraFields { document_id: Value, value: Value },
    #[error("Could not find latitude nor longitude in the document with the id: `{document_id}`. Was expecting `_geo.lat` and `_geo.lng` fields.")]
    MissingLatitudeAndLongitude { document_id: Value },
    #[error("Could not find latitude in the document with the id: `{document_id}`. Was expecting a `_geo.lat` field.")]
    MissingLatitude { document_id: Value },
    #[error("Could not find longitude in the document with the id: `{document_id}`. Was expecting a `_geo.lng` field.")]
    MissingLongitude { document_id: Value },
    #[error("Could not parse latitude nor longitude in the document with the id: `{document_id}`. Was expecting finite numbers but instead got `{lat}` and `{lng}`.")]
    BadLatitudeAndLongitude { document_id: Value, lat: Value, lng: Value },
    #[error("Could not parse latitude in the document with the id: `{document_id}`. Was expecting a finite number but instead got `{value}`.")]
    BadLatitude { document_id: Value, value: Value },
    #[error("Could not parse longitude in the document with the id: `{document_id}`. Was expecting a finite number but instead got `{value}`.")]
    BadLongitude { document_id: Value, value: Value },
}

fn format_invalid_filter_distribution(
    invalid_facets_name: &BTreeSet<String>,
    valid_facets_name: &BTreeSet<String>,
) -> String {
    if valid_facets_name.is_empty() {
        return "этот индекс не имеет настроенных фильтруемых атрибутов.".into();
    }

    let mut result = String::new();

    match invalid_facets_name.len() {
        0 => (),
        1 => write!(
            result,
            "атрибут `{}` не настроен для фильтрации.",
            invalid_facets_name.first().unwrap()
        )
        .unwrap(),
        _ => write!(
            result,
            " атрибуты `{}` не настроен для фильтрации.",
            invalid_facets_name.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", ")
        )
        .unwrap(),
    };

    match valid_facets_name.len() {
        1 => write!(
            result,
            " Атрибут, настроеные для фильтрации `{}`.",
            valid_facets_name.first().unwrap()
        )
        .unwrap(),
        _ => write!(
            result,
            " Атрибуты, настроеные для фильтрации `{}`.",
            valid_facets_name.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(", ")
        )
        .unwrap(),
    }

    result
}

/// A little macro helper to autogenerate From implementation that needs two `Into`.
/// Given the following parameters: `error_from_sub_error!(FieldIdMapMissingEntry => InternalError)`
/// the macro will create the following code:
/// ```ignore
/// impl From<FieldIdMapMissingEntry> for Error {
///     fn from(error: FieldIdMapMissingEntry) -> Error {
///         Error::from(InternalError::from(error))
///     }
/// }
/// ```
macro_rules! error_from_sub_error {
    () => {};
    ($sub:ty => $intermediate:ty) => {
        impl From<$sub> for Error {
            fn from(error: $sub) -> Error {
                Error::from(<$intermediate>::from(error))
            }
        }
    };
    ($($sub:ty => $intermediate:ty $(,)?),+) => {
        $(error_from_sub_error!($sub => $intermediate);)+
    };
}

error_from_sub_error! {
    FieldIdMapMissingEntry => InternalError,
    fst::Error => InternalError,
    documents::Error => InternalError,
    str::Utf8Error => InternalError,
    ThreadPoolBuildError => InternalError,
    SerializationError => InternalError,
    GeoError => UserError,
    CriterionError => UserError,
}

impl<E> From<grenad::Error<E>> for Error
where
    Error: From<E>,
{
    fn from(error: grenad::Error<E>) -> Error {
        match error {
            grenad::Error::Io(error) => Error::IoError(error),
            grenad::Error::Merge(error) => Error::from(error),
            grenad::Error::InvalidCompressionType => {
                Error::InternalError(InternalError::GrenadInvalidCompressionType)
            }
            grenad::Error::InvalidFormatVersion => {
                Error::InternalError(InternalError::GrenadInvalidFormatVersion)
            }
        }
    }
}

impl From<DocumentsBatchCursorError> for Error {
    fn from(error: DocumentsBatchCursorError) -> Error {
        match error {
            DocumentsBatchCursorError::Grenad(e) => Error::from(e),
            DocumentsBatchCursorError::SerdeJson(e) => Error::from(InternalError::from(e)),
        }
    }
}

impl From<Infallible> for Error {
    fn from(_error: Infallible) -> Error {
        unreachable!()
    }
}

impl From<HeedError> for Error {
    fn from(error: HeedError) -> Error {
        use self::Error::*;
        use self::InternalError::*;
        use self::SerializationError::*;
        use self::UserError::*;

        match error {
            HeedError::Io(error) => Error::from(error),
            HeedError::Mdb(MdbError::MapFull) => UserError(MaxDatabaseSizeReached),
            HeedError::Mdb(MdbError::Invalid) => UserError(InvalidStoreFile),
            HeedError::Mdb(error) => InternalError(Store(error)),
            // TODO use the encoding
            HeedError::Encoding(_) => InternalError(Serialization(Encoding { db_name: None })),
            HeedError::Decoding(_) => InternalError(Serialization(Decoding { db_name: None })),
            HeedError::InvalidDatabaseTyping => InternalError(InvalidDatabaseTyping),
            HeedError::DatabaseClosing => InternalError(DatabaseClosing),
            HeedError::BadOpenOptions { .. } => UserError(InvalidLmdbOpenOptions),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FaultSource {
    User,
    Runtime,
    Bug,
    Undecided,
}

impl std::fmt::Display for FaultSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FaultSource::User => "user error",
            FaultSource::Runtime => "runtime error",
            FaultSource::Bug => "coding error",
            FaultSource::Undecided => "error",
        };
        f.write_str(s)
    }
}

#[test]
fn conditionally_lookup_for_error_message() {
    let prefix = "Attribute `name` is not sortable.";
    let messages = vec![
        (BTreeSet::new(), "This index does not have configured sortable attributes."),
        (BTreeSet::from(["age".to_string()]), "Available sortable attributes are: `age`."),
    ];

    for (list, suffix) in messages {
        let err = UserError::InvalidSortableAttribute {
            field: "name".to_string(),
            valid_fields: list,
        };

        assert_eq!(err.to_string(), format!("{} {}", prefix, suffix));
    }
}
