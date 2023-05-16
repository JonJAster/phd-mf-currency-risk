using
    DataFrames,
    Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "domicile-grouped/info")
const OUTPUT_DIR = joinpath(DIRS.fund, "info")

const UNIQUE_AGG_COLS = [
    "domicile", "base-currency", "fund-standard-name", "fund-legal-name",
    "global-broad-category-group", "management-approach---active",
    "management-approach---passive", "true-no-load"
]

function main()
    time_start = time()
    println("Reading data...")
    fundsec_info = load_data_in_parts(INPUT_DIR)
    dropmissing!(fundsec_info, :fundid)

    println("Aggregating to fund level...")
    fund_info = groupby(fundsec_info, :fundid) |> gb->combine(
        gb, UNIQUE_AGG_COLS .=> agg_if_unique, "inception-date" => minimum; renamecols=false
    )

    output_filestring = makepath(OUTPUT_DIR, "mf_info.arrow")
    Arrow.write(output_filestring, fund_info)

    duration_s = round(time() - time_start, digits=2)
    println("Finished aggregating fund info in $duration_s seconds.")
end

agg_if_unique(v) = length(unique(v)) == 1 ? first(v) : missing

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end