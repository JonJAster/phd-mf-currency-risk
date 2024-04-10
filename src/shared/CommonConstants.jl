module CommonConstants

using DataStructures

export BETA_LAGS
export MIN_REGRESSION_OBS
export DEFAULT_DECAY
export TIMEWEIGHT_LAGS
export FLOW_CONTROL_LAGS
export DIRS
export EQUITY_LMS_FACTORS
export MODELS

const BETA_LAGS = 60
const MIN_REGRESSION_OBS = 36
const DEFAULT_DECAY = 0.186
const TIMEWEIGHT_LAGS = 18
const FLOW_CONTROL_LAGS = 19

const DIRS = (
    mf = (
        raw = "data/mutual-funds/raw",
        init = "data/mutual-funds/init",
        refined = "data/mutual-funds/refined"
    ),
    fx = (
        raw = "data/currencies/raw",
        refined = "data/currencies/refined",
        factors = "data/currencies/factors"
    ),
    eq = (
        raw = "data/equity-factors/raw",
        refined = "data/equity-factors/refined",
        factors = "data/equity-factors/factors"
    ),
    combo = (
        factors = "data/combined/factors",
        return_betas = "data/combined/return-betas",
        decomposed = "data/combined/decomposed",
        weighted = "data/combined/weighted",
        flow_betas = "data/combined/flow-betas"
    ),
    map = (
        raw = "data/maps/raw",
        refined = "data/maps/refined"
    ),
    test = "data/test"
)

const EQUITY_LMS_FACTORS = Dict(
    "market_equity" => "smb",
    "be_me" => "hml",
    "ret_12_1" => "wml",
    "ope_be" => "rmw",
    "at_gr1" => "cma"
)

const MODELS = OrderedDict(
    :ff_usa_capm => ("ff_usa", [:mkt]),
    :ff_usa_capm_lrv => ("ff_usa", [:mkt, :rx, :hml_fx]),
    :ff_usa_capm_ver => ("ff_usa", [:mkt, :dollar, :carry]),
    :ff_usa_ff3 => ("ff_usa", [:mkt, :smb, :hml]),
    :ff_usa_ff3_lrv => ("ff_usa", [:mkt, :smb, :hml, :rx, :hml_fx]),
    :ff_usa_ff3_ver => ("ff_usa", [:mkt, :smb, :hml, :dollar, :carry]),
    :ff_usa_ffc6 => ("ff_usa", [:mkt, :smb, :hml, :wml, :rmw, :cma]),
    :ff_usa_ffc6_lrv => (
        "ff_usa", [:mkt, :smb, :hml, :wml, :rmw, :cma, :rx, :hml_fx]
    ),
    :ff_usa_ffc6_ver => (
        "ff_usa", [:mkt, :smb, :hml, :wml, :rmw, :cma, :dollar, :carry]
    ),
    :ff_dev_capm => ("ff_dev", [:mkt]),
    :ff_dev_capm_lrv => ("ff_dev", [:mkt, :rx, :hml_fx]),
    :ff_dev_capm_ver => ("ff_dev", [:mkt, :dollar, :carry]),
    :ff_dev_ff3 => ("ff_dev", [:mkt, :smb, :hml]),
    :ff_dev_ff3_lrv => ("ff_dev", [:mkt, :smb, :hml, :rx, :hml_fx]),
    :ff_dev_ff3_ver => ("ff_dev", [:mkt, :smb, :hml, :dollar, :carry]),
    :ff_dev_ffc6 => ("ff_dev", [:mkt, :smb, :hml, :wml, :rmw, :cma]),
    :ff_dev_ffc6_lrv => (
        "ff_dev", [:mkt, :smb, :hml, :wml, :rmw, :cma, :rx, :hml_fx]
    ),
    :ff_dev_ffc6_ver => (
        "ff_dev", [:mkt, :smb, :hml, :wml, :rmw, :cma, :dollar, :carry]
    ),
    :jkp_usa_capm => ("jkp_usa", [:mkt]),
    :jkp_usa_capm_lrv => ("jkp_usa", [:mkt, :rx, :hml_fx]),
    :jkp_usa_capm_ver => ("jkp_usa", [:mkt, :dollar, :carry]),
    :jkp_usa_ff3 => ("jkp_usa", [:mkt, :smb, :hml]),
    :jkp_usa_ff3_lrv => ("jkp_usa", [:mkt, :smb, :hml, :rx, :hml_fx]),
    :jkp_usa_ff3_ver => ("jkp_usa", [:mkt, :smb, :hml, :dollar, :carry]),
    :jkp_usa_ffc6 => ("jkp_usa", [:mkt, :smb, :hml, :wml, :rmw, :cma]),
    :jkp_usa_ffc6_lrv => (
        "jkp_usa", [:mkt, :smb, :hml, :wml, :rmw, :cma, :rx, :hml_fx]
    ),
    :jkp_usa_ffc6_ver => (
        "jkp_usa", [:mkt, :smb, :hml, :wml, :rmw, :cma, :dollar, :carry]
    ),
    :jkp_dev_capm => ("jkp_dev", [:mkt]),
    :jkp_dev_capm_lrv => ("jkp_dev", [:mkt, :rx, :hml_fx]),
    :jkp_dev_capm_ver => ("jkp_dev", [:mkt, :dollar, :carry]),
    :jkp_dev_ff3 => ("jkp_dev", [:mkt, :smb, :hml]),
    :jkp_dev_ff3_lrv => ("jkp_dev", [:mkt, :smb, :hml, :rx, :hml_fx]),
    :jkp_dev_ff3_ver => ("jkp_dev", [:mkt, :smb, :hml, :dollar, :carry]),
    :jkp_dev_ffc6 => ("jkp_dev", [:mkt, :smb, :hml, :wml, :rmw, :cma]),
    :jkp_dev_ffc6_lrv => (
        "jkp_dev", [:mkt, :smb, :hml, :wml, :rmw, :cma, :rx, :hml_fx]
    ),
    :jkp_dev_ffc6_ver => (
        "jkp_dev", [:mkt, :smb, :hml, :wml, :rmw, :cma, :dollar, :carry]
    )
)

end # module CommonConstants