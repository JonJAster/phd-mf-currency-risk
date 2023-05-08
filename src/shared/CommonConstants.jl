module CommonConstants

export
    DIRS,
    FIELD_FOLDERS,
    COUNTRY_GROUPS,
    BENCHMARK_MODELS,
    CURRENCYRISK_MODELS,
    COMPLETE_MODELS,
    RESULT_COLUMNS,
    DEFAULT_BETA_LAGS,
    DEFAULT_MIN_REGRESSION_OBS,
    DEFAULT_OPTIONS

const DIRS = (
    fund = "data/mutual-funds",
    currency = "data/currencies",
    equity = "data/equities",
    map = "data/mappings"
)

const FIELD_FOLDERS = [
    "info", "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]
const COUNTRY_GROUPS = Dict(
    "lux" => ["Luxembourg"],
    "kor" => ["South Korea"],
    "usa" => ["United States"],
    "can-chn-jpn" => ["Canada", "China", "Japan"],
    "irl-bra" => ["Ireland", "Brazil"],
    "gbr-fra-ind" => ["United Kingdom", "France", "India"],
    "esp-tha-aus-zaf-mex-aut-che" => [
        "Spain", "Thailand", "Australia", "South Africa",
        "Mexico", "Austria", "Switzerland"
    ]
)
const BENCHMARK_MODELS = Dict(
    :world_capm => [:MKT],
    :world_ff3 => [:MKT, :SMB, :HML],
    :world_ff5 => [:MKT, :SMB, :HML, :RMW, :CMA],
    :world_ffcarhart => [:MKT, :SMB, :HML, :WML],
    :world_ff6 => [:MKT, :SMB, :HML, :RMW, :CMA, :WML]
)
const CURRENCYRISK_MODELS = Dict(
    :lrv => [:hml_fx, :rx],
    :lrv_net => [:hml_fx_net, :rx_net],
    :verdelhan => [:carry, :dollar]
)
const COMPLETE_MODELS = (
    Iterators.product(keys(CURRENCYRISK_MODELS), keys(BENCHMARK_MODELS)) |> collect |> vec
)

const RESULT_COLUMNS = [Symbol("$(a)_$(b)_betas") for (a, b) in COMPLETE_MODELS]

const DEFAULT_BETA_LAGS = 60
const DEFAULT_MIN_REGRESSION_OBS = 36
const DEFAULT_OPTIONS = Dict(
    :currency_type => :local,
    :strict_eq => true
)

end