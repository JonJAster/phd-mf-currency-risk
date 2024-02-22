module CommonFunctions

using DataFrames
using Arrow
using CSV
using Dates
using StatsBase
using ShiftedArrays: lead, lag

include("CommonConstants.jl")
using .CommonConstants

export dirslist
export makepath
export qhead
export qscan
export qlookup
export loadarrow
export initialise_base_data
export printtime
export init_raw
export rolling_std
export drop_allmissing!
export regression_table

const FILE_SUFFIX = r"\.[a-zA-Z0-9]+$"

const REGRESSION_ARGS = [
    :plus_lags, :plus_lag, :lags, :lag, :categories, :cat, :time_fixed_effects, :tfe,
    :entity_fixed_effects, :efe
]
const PARAMETER_REGRESSION_ARGS = [
    :plus_lags, :plus_lag, :lags, :lag, :time_fixed_effects, :tfe
]
const NOCOLUMN_REGRESSION_ARGS = [:time_fixed_effects, :tfe, :entity_fixed_effects, :efe]

function dirslist()
    println("-- DIRS LIST --")
    println()
    println("----")
    category_names = Dict(
        :mf => "Mutual Funds",
        :fx => "Currencies",
        :eq => "Equities"
    )
    for category in keys(DIRS)
        println(category_names[category])
        for folder in keys(DIRS[category])
            println("DIRS.$category.$folder: $(DIRS[category][folder])")
        end
        println("----")
    end
end

function makepath(paths...)
    pathstring = joinpath(paths...)
    if match(FILE_SUFFIX, pathstring) |> isnothing
        dirstring = pathstring
    else
        dirstring = dirname(pathstring)
    end
    
    if !isdir(dirstring)
        mkpath(dirstring)
        println("Missing directory created: $dirstring")
    end

    return pathstring
end

function qhead(filename)
    data = Arrow.Table(filename)
    output = propertynames(data)
    return output
end

function qscan(filename)
    data = Arrow.Table(filename)
    println("------")
    for col in propertynames(data)
        println(col)
        println()
        describe(data[col])
        println("------")
    end
    return
end

function qlookup(id; data=false)
    if data
        filestring = joinpath(DIRS.mf.init, "mf-data.arrow")
        mf_data = loadarrow(filestring)
        output = mf_data[mf_data.fundid .== id, :]
        nrow(output) == 0 && (output = mf_data[mf_data.secid .== id, :])
        return output
    else
        filestring_raw = joinpath(DIRS.mf.raw, "info.csv")
        mf_info = init_raw(filestring_raw, info=true)
        output = mf_info[mf_info.fundid .== id, :]
        nrow(output) > 0 && return output

        filestring_refined = joinpath(DIRS.mf.refined, "mf-info.arrow")
        mf_info = loadarrow(filestring_refined)
        output = mf_info[mf_info.fundid .== id, :]
        return output
    end
end

function loadarrow(filename)
    arrow_table = Arrow.Table(filename)
    df = deepcopy(DataFrame(arrow_table))
    arrow_table = nothing
    return df
end

function initialise_base_data(model)
    mf_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    factors_filename = joinpath(DIRS.combo.factors, "factors.arrow")

    mf_data = loadarrow(mf_filename)
    factors_data = loadarrow(factors_filename)

    regression_factors = _prepare_factors(factors_data, model)
    output = innerjoin(mf_data, regression_factors, on=:date)

    return output
end

function _prepare_factors(factors_data, model)
    model_region = model[1]
    model_factors = model[2]

    region_condition = (
        factors_data.region .== model_region .||
        factors_data.region .== "FX"
    )

    factor_condition = in.(factors_data.factor, Ref(String.(model_factors)))

    regioned_factors = factors_data[region_condition .&& factor_condition, :]
    wide_factors = unstack(regioned_factors, :date, :factor, :ret)
    dropmissing!(wide_factors)

    return wide_factors
end

function printtime(
        task, start_time;
        process_subtask="", process_start_time=0, minutes=false
        )
    if isempty(process_subtask) ⊻ iszero(process_start_time)
        error("If any process parameters are supplied, all must be supplied")
    end
    timed_process = !isempty(process_subtask)

    duration_s = round(time() - start_time, digits=2)
    duration_m = round(duration_s / 60, digits=2)
    
    if !timed_process
        printout = "Finished $task in $duration_s seconds"
        minutes && (printout *= " ($duration_m minutes)")
    else
        process_duration_s = round(time() - process_start_time, digits=2)
        process_duration_m = round(process_duration_s / 60, digits=2)

        printout = "Finished $task for $process_subtask in $process_duration_s seconds"
        minutes && (printout *= " ($process_duration_m minutes)")
        printout *= ", total running time $duration_s seconds ($duration_m minutes)"
    end

    println(printout)
    return nothing
end

function init_raw(filepath; info=false)
    if info
        data = CSV.read(filepath, DataFrame; truestrings=["Yes"], falsestrings=["No"])
        _normalise_names!(data; info=true)
        _null_empty_strings!(data)
    else
        data = CSV.read(filepath, DataFrame; stringtype=String, groupmark=',')
        _normalise_names!(data)
        drop_allmissing!(data, dims=:cols)
        drop_allmissing!(data, Not([:name, :fundid, :secid]); dims=:rows)
    end
    return data
end

function rolling_std(data, col, window; lagged)
    rolling_std = Vector{Union{Missing, Float64}}(missing, size(data, 1))

    for i in 1:size(data, 1)
        if lagged
            i <= window && continue
            window_start = i - window
            window_end = i - 1
        else
            i < window && continue
            window_start = i - window + 1
            window_end = i
        end

        data[window_start, :fundid] != data[window_end, :fundid] && continue

        start_date = data[window_end, :date] - Month(window-1)
        data[window_start, :date] != start_date && continue
        rolling_std[i] = std(data[window_start:window_end, col])
    end

    return rolling_std
end

drop_allmissing!(df; dims=1) = drop_allmissing!(df, propertynames(df); dims=dims)
function drop_allmissing!(df, cols; dims=1)
    if dims ∉ [1, 2, :row, :rows, :col, :cols]
        error("dims must be :rows or :cols")
    end

    dimsmap = Dict(:row => 1, :rows => 1, :col => 2, :cols => 2)
    if dims ∉ [1, 2]
        dims = dimsmap[dims]
    end

    testdf = DataFrame(
        name = ["A", "B", "C", "D", "E"],
        fundid = [1, 2, 3, 4, 5],
        secid = [1, 2, 3, 4, 5],
        date = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05"],
        value = [missing, missing, missing, missing, missing]
    )
    df = copy(testdf)
    cols = names(df)
    mask_matrix = .!(Matrix(df[!, cols]) .|> ismissing)
    if dims == 1
        one_vector = ones(size(mask_matrix,2))
        all_missing = mask_matrix * one_vector .== zero(size(mask_matrix,1))
        delete!(df, findall(all_missing))
    else
        one_vector = ones(size(mask_matrix,1))
        all_missing = mask_matrix' * one_vector .== zero(size(mask_matrix,2))
        select!(df, Not(cols[all_missing]))
    end
end

function regression_table(data, entity_col, date_col, column_args...)
    """
    Returns a DataFrame containing columns to be used as inputs in a regression as
    defined by the column_args. The column_args can be any combination of any number of the 
    following sequences:
        - A column name in the data DataFrame.
        - A column name followed by a regression argument (see REGRESSION_ARGS).
        - A column name followed by a regression argument that takes a parameter followed
            by the parameter value (see PARAMETER_REGRESSION_ARGS).
        - A regression argument that takes no column name (see NOCOLUMN_REGRESSION_ARGS).

    Columns on their own will be included in the regression table as is. Columns followed
    by a regression argument will be included in the regression table with the regression
    argument applied to them or converted into a series of other columns, depending on the
    argument.

    Arguments
    ---------
    data : DataFrame
        The DataFrame containing the columns to be used in the regression.
    entity_col : Symbol
        The name of the column in data containing the entity identifiers.
    date_col : Symbol
        The name of the column in data containing the dates.
    column_args : Any
        Any number of column names, column names followed by regression arguments followed
        optionally by regression parameters, or regression arguments that take no column
        name.

    Returns
    -------
    regression_table : DataFrame
        A DataFrame containing the columns to be used in the regression.
    """
    data_cols = propertynames(data)
    reserved_usage = REGRESSION_ARGS ∩ data_cols
    reserved_usage != [] && error("Reserved args used as column names: $reserved_usage.")

    regression_table = select(data, entity_col, date_col)
    temporary_column_names = [date_col => :date, entity_col => :entity]
    rename!(regression_table, temporary_column_names)
    !issorted(regression_table, [:entity, :date]) && sort!(
        regression_table, [:entity, :date]
    )

    active_column = nothing
    active_arg_call = nothing
    for arg in column_args
        if !isnothing(active_arg_call)
            if arg ∈ REGRESSION_ARGS ∪ data_cols
                _do_arg_call!(active_arg_call, regression_table, active_column)
                active_column, active_arg_call = nothing, nothing
            else
                _do_arg_call!(
                    active_arg_call, regression_table, active_column; parameter=arg
                )
                active_column, active_arg_call = nothing, nothing
                continue
            end
        end

        if arg ∈ data_cols
            active_column = arg
            regression_table[!, arg] = data[:, arg]
            continue
        end

        arg ∈ REGRESSION_ARGS || error(
            "$arg is not a valid column name or regression argument."
        )

        !isnothing(active_column) || arg ∈ NOCOLUMN_REGRESSION_ARGS || error(
            "No column selected for $arg."
        )
        
        if arg ∈ PARAMETER_REGRESSION_ARGS
            active_arg_call = arg
            continue
        end

        _do_arg_call!(arg, regression_table, active_column)
        active_column = nothing
    end

    rename!(regression_table, reverse.(temporary_column_names))

    return regression_table
end

function _do_arg_call!(arg, data, col; parameter=nothing)
    if arg == :lags || arg == :lag
        _add_lags!(data, col, nlags=parameter)
        select!(data, Not(col))
    elseif arg == :plus_lags || arg == :plus_lag
        _add_lags!(data, col, nlags=parameter)
    elseif arg == :categories || arg == :cat
        _convert_to_category_dummies!(data, col)
    elseif arg == :time_fixed_effects || arg == :tfe
        _add_time_fe!(data, frequency=parameter)
    elseif arg == :entity_fixed_effects || arg == :efe
        _add_entity_fe!(data)
    end
end

function _add_lags!(data, col; nlags)
    isnothing(nlags) && (nlags = 1)
    typeof(nlags) <: Integer || error("Number of lags must be an integer.")
    gb = groupby(data, :entity)

    for i in 1:nlags
        transform!(gb, col => (col->lag(col, i)) => "$(col)_lag$i")
    end
end

function _convert_to_category_dummies!(data, col)
    categories = unique(data[!, col])[2:end]
    for category in categories
        data[!, "$(col)_$category"] = Int.(data[!, col] .== category)
    end
    select!(data, Not(col))
end

function _add_time_fe!(data; frequency)
    if isnothing(frequency)
        date_category = :fe_date_enum
        unique_dates_indexer = (
            unique(data.date) |> enumerate |> collect .|> reverse |> Dict
        )
        data[!, date_category] = get.(Ref(unique_dates_indexer), data.date, nothing)
    elseif frequency ∈ [:d, :day, :daily]
        date_category = :fe_date
        data[!, date_category] = Dates.format.(data.date, "yyyymmdd")
    elseif frequency ∈ [:m, :month, :monthly]
        date_category = :fe_month
        data[!, date_category] = Dates.format.(data.date, "yyyymm")
    elseif frequency ∈ [:q, :quarter, :quarterly]
        date_category = :fe_quarter
        yearstr = string.(Dates.year.(data.date))
        quarterstr = string.(Dates.quarterofyear.(data.date))
        data[!, date_category] = String.(yearstr) .* "Q" .* String.(quarterstr)
    elseif frequency ∈ [:y, :year, :yearly]
        date_category = :fe_year
        data[!, date_category] = Dates.format.(data.date, "yyyy")
    else
        error("Invalid frequency: $frequency. Must be :month, :quarter, or :year.")
    end

    _convert_to_category_dummies!(data, date_category)
end

function _add_entity_fe!(data)
    data[!, :fe_entity] = data[!, :entity]
    _convert_to_category_dummies!(data, :fe_entity)
end

function _normalise_names!(df; info=false)
    if info
        re_invalidchars_nonend = r"[^a-zA-Z0-9]+(?!$)"
        re_invalidchars_end = r"[^a-zA-Z0-9]+$"

        function namemap(x)
            replace(x, re_invalidchars_nonend => "_") |> x ->
            replace(x, re_invalidchars_end => "") |>
            lowercase
        end

        new_names = names(df) .|> namemap
        rename!(df, new_names)
    else
        n_id_cols = 3
        n_date_cols = ncol(df) - n_id_cols

        id_cols = names(df)[1:n_id_cols] .|> lowercase
        
        re_fieldname = r"^.+(?=\s?\r?\n\d{4}-\d{2})"
        re_date = r"(?<=\n)\d{4}-\d{2}"
        
        fieldname_match = match(re_fieldname, names(df)[n_id_cols + 1]).match
        fieldname = replace(fieldname_match, r"\r|\n| $" => "")
        
        start_date = match(re_date, names(df)[n_id_cols + 1]).match |> Dates.Date
        last_date = match(re_date, last(names(df))).match |> Dates.Date
        date_cols = [_offset_monthend(start_date, i) for i in 0:n_date_cols-1]

        last(date_cols) != last_date && @warn(
            "The calculated end date ($(last(date_cols))) is not the same as the last date " *
            "in the dataset for $fieldname ($last_date). This suggests that some date " *
            "columns may be missing or incorrectly sequenced."
        )

        rename!(df, Symbol.([id_cols; date_cols]))
    end
    return
end

function _null_empty_strings!(df)
    for col in propertynames(df)
        if count(coalesce.(df[!, col] .== "", false)) > 0
            df[!, col] = replace(df[!, col], "" => missing)
        end
    end
    return
end

end # module CommonFunctions
