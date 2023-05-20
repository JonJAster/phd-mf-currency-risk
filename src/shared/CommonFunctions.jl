module CommonFunctions

using
    DataFrames,
    Arrow,
    CSV,
    Base.Threads,
    Dates

using ShiftedArrays: lag

include("CommonConstants.jl")
using .CommonConstants

export
    makepath,
    push_with_currency_code!,
    option_foldername,
    group_transform!,
    group_transform,
    group_combine,
    regression_table,
    infofilter,
    infolookup,
    load_data_in_parts,
    ismissing_or_blank,
    name_model,
    offset_monthend,
    nonmissing

const REGRESSION_ARGS = [
    :plus_lags, :plus_lag, :lags, :lag, :categories, :cat, :time_fixed_effects, :tfe,
    :entity_fixed_effects, :efe
]
const PARAMETER_REGRESSION_ARGS = [
    :plus_lags, :plus_lag, :lags, :lag, :time_fixed_effects, :tfe
]
const NOCOLUMN_REGRESSION_ARGS = [:time_fixed_effects, :tfe, :entity_fixed_effects, :efe]

function makepath(paths...)
    filestring = joinpath(paths...)
    dirstring = dirname(filestring)
    
    if !isdir(dirstring)
        mkpath(dirstring)
        println("Missing directory created: $dirstring")
    end

    return filestring
end

function push_with_currency_code!(datalist, df, currency_code, value_columns)
    append_data = copy(df)
    append_data.currency .= currency_code
    append_data = append_data[:, [:currency, :date, value_columns]]
    push!(datalist, append_data)
end

function option_foldername(; currency_type, kwargs...)
    options_in = Dict{Symbol, Any}()
    for (key, value) in kwargs
        typeof(value) <: AbstractString && (value = Symbol(value))
        options_in[key] = value
    end

    if currency_type == :local
        folder_name = "local-rets"
    elseif currency_type == :usd
        folder_name = "usd-rets"
    else
        error("Invalid currency type: $currency_type. Must be :local or :usd.")
    end

    if haskey(options_in, :raw_ret_only) && options_in[:raw_ret_only] == false
        folder_name *= "_gret-filled"
    end

    if haskey(options_in, :polation_method)
        if options_in[:polation_method] == :both
            folder_name *= "_na-int-exp"
        elseif options_in[:polation_method] == :interpolate
            folder_name *= "_na-int"
        elseif options_in[:polation_method] == :extrapolate
            folder_name *= "_na-exp"
        elseif options_in != false
            error("Invalid polation method: $(options_in[:polation_method]). Must be :both, "*
                  ":interpolate, :extrapolate, or false.")
        end
    end
    
    if haskey(options_in, :strict_eq) && options_in[:strict_eq] == true
        if haskey(options_in, :exc_finre) && options_in[:exc_finre] == true
            folder_name *= "_eq-strict-exfinre"
        else
            folder_name *= "_eq-strict"
        end
    elseif haskey(options_in, :exc_finre) && options_in[:exc_finre] == true
        folder_name *= "_eq-exfinre"
    end

    if haskey(options_in, :inv_targets) && options_in[:inv_targets] == true
        folder_name *= "_targets"
    end

    if haskey(options_in, :age_filter) && options_in[:age_filter] == true
        folder_name *= "_age-filtered"
    end

    return folder_name
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
                do_arg_call!(active_arg_call, regression_table, active_column)
                active_column, active_arg_call = nothing, nothing
            else
                do_arg_call!(
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

        do_arg_call!(arg, regression_table, active_column)
        active_column = nothing
    end

    rename!(regression_table, reverse.(temporary_column_names))

    return regression_table
end

function do_arg_call!(arg, data, col; parameter=nothing)
    if arg == :lags || arg == :lag
        add_lags!(data, col, nlags=parameter)
        select!(data, Not(col))
    elseif arg == :plus_lags || arg == :plus_lag
        add_lags!(data, col, nlags=parameter)
    elseif arg == :categories || arg == :cat
        convert_to_category_dummies!(data, col)
    elseif arg == :time_fixed_effects || arg == :tfe
        add_time_fe!(data, frequency=parameter)
    elseif arg == :entity_fixed_effects || arg == :efe
        add_entity_fe!(data)
    end
end

function add_lags!(data, col; nlags)
    isnothing(nlags) && (nlags = 1)
    typeof(nlags) <: Integer || error("Number of lags must be an integer.")
    gb = groupby(data, :entity)

    for i in 1:nlags
        transform!(gb, col => (col->lag(col, i)) => "$(col)_lag$i")
    end
end

function convert_to_category_dummies!(data, col)
    categories = unique(data[!, col])[2:end]
    for category in categories
        data[!, "$(col)_$category"] = Int.(data[!, col] .== category)
    end
    select!(data, Not(col))
end

function add_time_fe!(data; frequency)
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

    convert_to_category_dummies!(data, date_category)
end

function add_entity_fe!(data)
    data[!, :fe_entity] = data[!, :entity]
    convert_to_category_dummies!(data, :fe_entity)
end

function infofilter(f, data; notebook=false)
    fund_info = _loadinfo(notebook)
    
    filtered_info = filter(f, fund_info)
    filtered_ids = filtered_info.fundid |> Set

    return filter(:fundid => in(filtered_ids), data)
end

function infolookup(id; notebook=false)
    fund_info = _loadinfo(notebook)
    idrow = fund_info[fund_info.fundid .== id, :]
    
    for col in names(fund_info)
        println("$col: $(idrow[1, col])")
    end

    return idrow
end

function _loadinfo(notebook)
    info_filename = joinpath(DIRS.fund, "info", "mf_info.arrow")
    notebook && (info_filename = joinpath("..", info_filename))
    fund_info = Arrow.Table(info_filename) |> DataFrame
    return fund_info
end

function group_transform!(df, group_cols, input_cols, f::Function, output_cols)
    groupby(df, group_cols) |> x -> transform!(x, input_cols => f => output_cols)
end

function group_transform(df, group_cols, input_cols, f::Function, output_cols)
    output = groupby(df, group_cols) |> x -> transform(x, input_cols => f => output_cols)

    return output
end

function group_combine(df, group_cols, input_cols, f::Function, output_cols;
                               cast=true)
    if cast
        output = (
            groupby(df, group_cols) |> x -> combine(x, input_cols .=> f .=> output_cols)
        )
    else
        output = (
            groupby(df, group_cols) |> x -> combine(x, input_cols => f => output_cols)
        )
    end

    return output
end

function load_data_in_parts(dirstring; select=nothing)
    output_data = DataFrame[]

    for file in readdir(dirstring)
        filestring = joinpath(dirstring, file)
        file_data = CSV.read(filestring, DataFrame, select=select)
        push!(output_data, file_data)
    end

    return vcat(output_data...)
end

name_model(model) = "$(model[1])_$(model[2])"
offset_monthend(date, offset=1) = date + Dates.Month(offset) |> Dates.lastdayofmonth
ismissing_or_blank(x) = ismissing(x) || x == ""
nonmissing(v) = coalesce.(v, false)

end