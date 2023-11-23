using DataFrames
using Arrow
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const OUTPUT_DIR = joinpath(DIRS.fund, "post-processing")

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    time_start = time()
    println("Reading data...")
    
    filepath = joinpath(INPUT_DIR, options_folder, "main/fund_data.arrow")
    df_funds = Arrow.Table(filepath) |> DataFrame

    # Current dynamic filters (BHO):
    # 1. Start date >= Jan 1991
    # 2. End date <= Dec 2011
    # 3. Domestic only

    println("Filtering data...")
    date_filter = notoutside.(df_funds.date, Date(1991, 1, 1), Date(2011, 12, 31))
    international_fund_filter = (
        ismissing.(df_funds.inv_international) .|| df_funds.inv_international != 1
    )

    df_funds = df_funds[date_filter .& international_fund_filter, :]

    output_filestring = makepath(
        OUTPUT_DIR, options_folder, "main/filtered_fund_data.arrow"
    )
    Arrow.write(output_filestring, df_funds)

    duration_s = round(time() - time_start, digits=2)
    println("Finished filtering mutual fund data in $duration_s seconds.")
end

function notoutside(date, start_date, end_date)
    !ismissing(start_date) && date < start_date && return false
    !ismissing(end_date) && date > end_date && return false
    return true
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end