using Revise
using DataFrames
using CSV
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function aggregate_mf_info()
    task_start = time()
    filepath = joinpath(DIRS.mf.raw, "info.csv")
    
    mf_info = init_raw(filepath, info=true)

    mf_info_aggregated = _aggregate_info(mf_info)

    printtime("initialising mutual fund info", task_start, minutes=false)
    return mf_info_aggregated
end

function _aggregate_info(mf_info)
    match_if_equal(x) = length(unique(x)) == 1 ? first(x) : missing

    output = combine(
        groupby(mf_info, :fundid),
        :fund_standard_name => match_if_equal => :fund_standard_name,
        :fund_legal_name => match_if_equal => :fund_legal_name,
        :global_category => match_if_equal => :global_category,
        :morningstar_category => match_if_equal => :morningstar_category,
        :us_category_group => match_if_equal => :us_category_group,
        :investment_area => match_if_equal => :investment_area,
        :true_no_load => all => :true_no_load,
        :inception_date => minimum => :inception_date
    )

    return output
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = aggregate_mf_info()
    output_filename = makepath(DIRS.mf.refined, "mf-info.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing mutual fund info", task_start, minutes=false)
end