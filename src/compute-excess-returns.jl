using DataFrames
using CSV
using Arrow
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR_FUND = joinpath(DIRS.fund, "post-processing")
const INPUT_DIR_RF = joinpath(DIRS.equity, "raw")

const OUTPUT_DIR = INPUT_DIR_FUND

const OUTPUT_COLUMNS = [
    :fundid, :date, :nonex_ret, :ex_ret, :fund_flow, :fund_assets, :mean_costs, :fund_age,
    :inv_international, :fin_or_re
]

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...); filtered=false)
    time_start = time()

    println("Reading data...")
    filtered ? parent_dir = "filtered" : parent_dir = "main"
    filestring_funds = joinpath(
        INPUT_DIR_FUND, options_folder, parent_dir, "fund_data.arrow"
    )
    filestring_riskfree = joinpath(INPUT_DIR_RF, "usd_riskfree.csv")

    df_funds = Arrow.Table(filestring_funds) |> DataFrame
    df_riskfree = CSV.read(
        filestring_riskfree, DataFrame, dateformat=DateFormat("dd/mm/yyyy")
    )

    println("Computing excess returns...")
    df_combined = innerjoin(df_funds, df_riskfree, on=:date)
    df_combined.ex_ret = df_combined.ret .- df_combined.rf
    rename!(df_combined, :ret => :nonex_ret)

    select!(df_combined, OUTPUT_COLUMNS)

    output_filestring = joinpath(
        OUTPUT_DIR, options_folder, parent_dir, "excess_fund_data.arrow"
    )
    Arrow.write(output_filestring, df_combined)

    duration_s = round(time() - time_start, digits=2)
    println("Finished computing excess returns in $duration_s seconds.")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
    main(; filtered=true)
end