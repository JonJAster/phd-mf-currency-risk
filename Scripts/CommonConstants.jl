module CommonConstants

export 
    BENCHMARK_MODELS,
    CURRENCYRISK_MODELS,
    COMPLETE_MODELS,
    RESULT_COLUMNS,
    DEFAULT_BETA_LAGS,
    DEFAULT_MIN_REGRESSION_OBS

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

const DEFAULT_BETA_LAGS = 24
const DEFAULT_MIN_REGRESSION_OBS = 12

end