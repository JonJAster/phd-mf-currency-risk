using DataStructures

const DIRS = (
    fund = "data/mutual-funds",
    currency = "data/currencies",
    equity = "data/equities",
    map = "data/mappings"
)

const QFILES = (
    fundinfo = ["data/mutual-funds/info/mf_info.arrow"]
)

const BENCHMARK_MODELS = OrderedDict(
    :world_capm => [:mkt],
    :world_ff3 => [:mkt, :smb, :hml],
    :world_ff5 => [:mkt, :smb, :hml, :rmw, :cma],
    :world_ffcarhart => [:mkt, :smb, :hml, :wml],
    :world_ff6 => [:mkt, :smb, :hml, :rmw, :cma, :wml]
)

const CURRENCYRISK_MODELS = OrderedDict(
    :lrv => [:hml_fx, :rx],
    :lrv_net => [:hml_fx_net, :rx_net],
    :verdelhan => [:carry, :dollar]
)

const COMPLETE_MODELS = (
    Iterators.product(keys(BENCHMARK_MODELS), keys(CURRENCYRISK_MODELS)) |>
    collect |> permutedims |> vec
)

const RESULT_COLUMNS = [Symbol("$(a)_$(b)_betas") for (a, b) in COMPLETE_MODELS]

const DEFAULT_BETA_LAGS = 60
const DEFAULT_MIN_REGRESSION_OBS = 36
const DEFAULT_DECAY = 0.186
const DEFAULT_TIMEWEIGHT_LAGS = 18
const DEFAULT_OPTIONS = Dict(
    :currency_type => :local,
    :strict_eq => true,
    :polation_method => :interpolate,
    :inv_targets => true
)