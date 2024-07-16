module CommonFunctions

using DataFrames
using Arrow
using CSV
using REPL
using LibPQ
using Tables
using Dates
using StatsBase
using Crayons
using ColorSchemes
using ShiftedArrays: lead, lag

include("CommonConstants.jl")
using .CommonConstants

export bho_dates_only
export connect_wrds, query_wrds, scan_libraries_wrds, scan_sets_wrds, scan_vars_wrds
export datastep, datastepper, stepclass, stepgroup, stepfund
export waitkey
export dirslist
export drop_allmissing!
export fundclass, fundgroup, fund
export filter_fundids
export init_raw, initialise_base_data, initialise_flow_data
export inspect
export investment_target_is
export loadarrow
export makepath
export nonmissing, withmissing
export post_bho_only
export pprint
export printtime
export proportion
export qhead, qlookup, qscan
export regression_table
export rolling_std

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

fundclass(data, id::AbstractVector) = data[nonmissing(in.(data.fund_class_id, Ref(id))), :]
fundclass(data, id) = data[nonmissing(data.fund_class_id .== id), :]
fundgroup(data, id::AbstractVector) = data[nonmissing(in.(data.fund_class_group_id, Ref(id))), :]
fundgroup(data, id) = data[nonmissing(data.fund_class_group_id .== id), :]
fund(data, id::AbstractVector) = data[nonmissing(in.(data.fund_id, Ref(id))), :]
fund(data, id) = data[nonmissing(data.fund_id .== id), :]

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

nonmissing(condition) = coalesce.(condition, false)
withmissing(condition) = coalesce.(condition, true)

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

function pprint(
        df_in,
        id_cols=nothing;
        color_by=nothing,
        rows=nothing,
        centre=false,
        header=true,
        spacer=false, 
        cluster_size=10,
        round_to=5
        )
    """
    Pretty prints a DataFrame in the terminal. The DataFrame is printed in clusters of rows
    with the columns split into sets that fit within the terminal width. The ID columns are
    printed first and the remaining columns are split into sets that fit within the terminal
    width. The sets are printed in sequence with a spacer line between each set.

    Parameters
    ----------
    df_in : DataFrame
        The DataFrame to be printed.
    id_cols : DataFrame column selector (Symbol, String, Vector, ALL, Not, Between, In, Regex)
        The column name in the data DataFrame containing the entity identifiers.
    color_by : DataFrame column selector (Symbol, String, Vector, ALL, Not, Between, In, Regex)
        If not nothing, rows are coloured on the basis of the values in this column.
    rows : Int
        The number of rows to be printed. If not specified, all rows are printed.
    centre : Bool
        If true, the text is centred within the column width. If false, the text is left
        aligned within the column width.
    header : Bool
        If true, the column names are printed as a header for each set of columns.
    spacer : Bool
        If true, a spacer elipsis line is printed at the end of the printed rows.
    cluster_size : Int
        The number of rows to be printed in each cluster.
    round_to : Int
        The number of decimal places to which floating values are rounded before printing.
    """

    MAKE_TEXT_WHITE = Crayon(foreground=(255,255,255))
    MAKE_TEXT_PURPLE = Crayon(foreground=(127,25,195))
    MAKE_TEXT_LIGHT_GREY = Crayon(foreground=(200,200,200))
    MAKE_TEXT_DARK_GREY = Crayon(foreground=(10,10,10))
    ITR_VALUE = 1
    ITR_NEXT_INDEX = 2

    isnothing(id_cols) && (id_cols = [first(propertynames(df))])
    typeof(id_cols) <: AbstractArray || (id_cols = [id_cols])
    isnothing(rows) && (rows = nrow(df))
    terminal_width = displaysize(stdout)[2]
    
    df = deepcopy(df_in[1:rows,:])
    if !isnothing(color_by)
        row_colorwheel = Iterators.cycle(ColorSchemes.tab20.colors) |> Iterators.Stateful
        # row_color = iterate(row_colorwheel)[ITR_VALUE]
        # (row_r, row_g, row_b) = Int.(floor.(255 .* (row_color.r, row_color.g, row_color.b)))
        # Crayon(foreground=(row_r, row_g, row_b))
    else
        row_colorwheel = nothing
    end

    floating_cols = [
        col for col in propertynames(df) if eltype(df[!, col]) <: AbstractFloat
    ]
    for col in floating_cols
        df[!, col] = round.(df[!, col], digits=round_to)
    end

    content_width(col_name) = maximum(length.(string.(df[!, col_name])))
    total_width(col_name) = maximum([length(string(col_name)), content_width(col_name)])

    stored_true_widths = Dict(col => total_width(col) for col in propertynames(df))

    function printwidth(cols) # cols = id_cols
        # Returns the print width of an array of column names
        isempty(cols) && return 0
        
        combined_col_widths = sum([stored_true_widths[col] for col in cols])
        n_cols_with_whitespace = length(cols) - 1
        whitespace_width = 2*n_cols_with_whitespace
        print_width = combined_col_widths + whitespace_width
        return print_width
    end

    if printwidth(id_cols) > terminal_width
        error("ID column width(s) exceed terminal width.")
    end

    non_id_cols = propertynames(df[!, Not(id_cols)])
    max_fittable_width = terminal_width - printwidth(id_cols) - 2
    fit_check = [printwidth([id_cols..., x]) <= max_fittable_width for x in non_id_cols]
    if any(.!fit_check)
        # Each column needs to at minimum fit next to the ID columns
        offending_cols = non_id_cols[.!fit_check]
        error("Column width exceeds terminal width: $offending_cols")
    end

    print_sets = []
    current_print_set = copy(id_cols)
    for col_name in non_id_cols # col_name = non_id_cols[1]
        if printwidth([current_print_set..., col_name]) > terminal_width
            push!(print_sets, deepcopy(current_print_set))
            current_print_set = [id_cols..., col_name]
        else
            push!(current_print_set, col_name)
        end
    end
    isempty(current_print_set) || push!(print_sets, current_print_set)

    function centre_text(text, width)
        text_length = length(text)
        padding = width - text_length
        left_padding = div(padding, 2)
        right_padding = padding - left_padding
        return " "^left_padding * text * " "^right_padding
    end

    function pad_text(text, width)
        text_length = length(text)
        padding = width - text_length
        return text * " "^padding
    end

    centre ? (align_text = centre_text) : (align_text = pad_text)
    
    function print_header(cols)
        TRAILING_WHITESPACE = 2
        printout = ""
        for col_name in cols
            printout *= align_text(
                string(col_name),
                stored_true_widths[col_name]
            )
            printout *= "  "
        end
        print(MAKE_TEXT_LIGHT_GREY)
        println(printout[1:end-TRAILING_WHITESPACE])
        println("-"^printwidth(cols))
        print(MAKE_TEXT_WHITE)
    end

    function print_row(row, cols)
        TRAILING_WHITESPACE = 2
        printout = ""
        for col_name in cols
            ismissing(row[col_name]) && (printout *= string(MAKE_TEXT_DARK_GREY))
            printout *= align_text(
                string(row[col_name]),
                stored_true_widths[col_name]
            )
            ismissing(row[col_name]) && (printout *= string(MAKE_TEXT_WHITE))
            printout *= "  "
        end
        println(printout[1:end-TRAILING_WHITESPACE])
    end

    cluster_size = (length(print_sets) == 1) ? rows : cluster_size

    last_printed_row = 0
    while(last_printed_row < rows)
        for print_set in print_sets
            header && print_header(print_set)
            
            for i in last_printed_row+1:min(last_printed_row+cluster_size, rows)
                print_row(df[i, :], print_set)
            end
        end

        last_printed_row += cluster_size
        if last_printed_row < rows
            print(MAKE_TEXT_PURPLE)
            println()
            println("*"^maximum(printwidth.(print_sets)))
            println()
            print(MAKE_TEXT_WHITE)
        end

        spacer && print(centre_text("...", maximum(printwidth.(print_sets))))
    end
end

function proportion(f, data, of; id=first(propertynames(data)))
    """
    Prints the proportion of values for which f(data[!, of]) is true both overall and by id.

    Parameters
    ----------
    f : Function
        The function to be applied to the data. f should take as many arguments as there are
        columns in of.
    data : DataFrame
        The DataFrame containing the data.
    of : DataFrame column selector (Symbol, String, Vector, ALL, Not, Between, In, Regex)
        The column name in the data DataFrame to which the function is applied.
    id : DataFrame column selector (Symbol, String, Vector, ALL, Not, Between, In, Regex)
        The column name in the data DataFrame containing the entity identifiers.

    Returns
    -------
    None
    """
    n_parameters_in_f = length(methods(f)[1].sig.parameters) - 1
    if length(of) != n_parameters_in_f
        error("Number of columns in 'of' does not match the number of arguments in 'f'.")
    end

    f_condition = f.(eachcol(data[!, of])...)
    matches = data[f_condition, :]
    
    f_count = nrow(matches)
    total_count = nrow(data)
    n_ids = length(unique(data[!, id]))
    n_f_ids = length(unique(matches[!, id]))

    println(
        "Condition matches $f_count ($(round(f_count/total_count*100, digits=2))%) " *
        "observations and $n_f_ids ($(round(n_f_ids/n_ids*100, digits=2))%) entities"
    )

    return nothing
end

function inspect(data, id; id_limit=5, window=6, skip_ids=0)
    id_list = unique(data[!,id])
    final_inspect = minimum([id_limit, length(id_list)])

    println("*******")
    println("INSPECT")
    println("*******")
    println()

    for i in id_list[1+skip_ids:final_inspect+skip_ids]
        id_data = data[data[!,id] .== i, :]
        slice_length = nrow(id_data)
        if slice_length <= window
            pprint(id_data)
        else
            full_upper_window = ceil(Int, window/2)
            full_lower_window = floor(Int, window/2)
            upper_window = minimum([ceil(Int, slice_length/2), full_upper_window])
            lower_window = minimum([floor(Int, slice_length/2), full_lower_window])

            pprint(first(id_data, upper_window), id; spacer=true)
            println()
            pprint(last(id_data, lower_window), id; header=false)
        end
        println()
    end
end

function connect_wrds(username=nothing, password=nothing)
    if isnothing(username) || isnothing(password)
        credentials_file = joinpath(DIRS.map.raw, "wrds-credentials.csv")
        credentials = CSV.read(credentials_file, DataFrame)
        isnothing(username) && (username = first(credentials.username))
        isnothing(password) && (password = first(credentials.password))
    end
    wrds = LibPQ.Connection(
        """
        host = wrds-pgdata.wharton.upenn.edu
        port = 9737
        user = '$username'
        password = '$password'
        sslmode = 'require' dbname = wrds
        """
    )
    return wrds
end

function scan_libraries_wrds(wrds)
    query = (
        "select distinct table_schema
        from information_schema.tables
        where table_type ='VIEW'
        or table_type ='FOREIGN TABLE'
        order by table_schema"
    )
    output = query_wrds(wrds, query)
    return output
end

function scan_sets_wrds(wrds, library)
    query = (
        "select table_name
        from information_schema.tables
        where table_schema = '$library'
        order by table_name"
    )
    output = query_wrds(wrds, query)
    return output
end

function scan_vars_wrds(wrds, library, dataset)
    query = (
        "select column_name
        from information_schema.columns
        where table_schema = '$library'
        and table_name = '$dataset'
        order by column_name"
    )
    output = query_wrds(wrds, query)
    return output
end

function query_wrds(wrds, query; limit=nothing, save_to=nothing)
    if !isnothing(limit) && !isnothing(save_to)
        println(
            "Warning: Attempted to saved limited file to disk. " *
            "Filename appended with '_limit$limit'."
        )

        # TODO: Handle save_to sent as full dir or with extension
        save_to *= "_limit$limit"
    end

    !isnothing(limit) && (query *= " limit $limit")

    data = LibPQ.execute(wrds, query) |> columntable |> DataFrame

    if !isnothing(save_to)
        filepath = makepath(DIRS.mf.raw, save_to, ".arrow")
        try
            Arrow.write(filepath, data)
        catch e # TODO: catch specific error
            println("Not written to file due to error: ", e)
        end
    end

    return data
end

function loadarrow(filename)
    arrow_table = Arrow.Table(filename)
    df = deepcopy(DataFrame(arrow_table))
    arrow_table = nothing
    return df
end

function initialise_base_data(model)
    mf_filename = joinpath(DIRS.mf.refined, "mf-excess-returns.arrow")
    factors_filename = joinpath(DIRS.combo.factors, "factors.arrow")

    mf_data = loadarrow(mf_filename)
    factors_data = loadarrow(factors_filename)

    regression_factors = _prepare_factors(factors_data, model)
    output = innerjoin(mf_data, regression_factors, on=:date)

    return output
end

function initialise_flow_data(model_name)
    filename_mf = joinpath(DIRS.mf.refined, "mf-excess-returns.arrow")
    filename_info = joinpath(DIRS.mf.refined, "mf-info.arrow")
    filename_decomposition = joinpath(DIRS.combo.weighted, "$model_name.arrow")

    fund_base_data = loadarrow(filename_mf)
    fund_info = loadarrow(filename_info)
    decomposed_returns = loadarrow(filename_decomposition)

    fund_base_data.std_return_12m = rolling_std(fund_base_data, :ex_ret, 12; lagged=true)

    select!(
        fund_base_data,
        [:fundid, :date, :flow, :net_assets_m1, :costs, :std_return_12m]
    )
    select!(fund_info, [:fundid, :true_no_load, :inception_date])

    fund_rets_data = outerjoin(fund_base_data, decomposed_returns, on=[:fundid, :date])

    fund_full_data = innerjoin(
        fund_rets_data, fund_info, on=:fundid
    )

    fund_full_data.age = (
        12*(year.(fund_full_data.date) .- year.(fund_full_data.inception_date)) .+
        (month.(fund_full_data.date) .- month.(fund_full_data.inception_date)) .+ 1
    )

    fund_full_data.log_size_m1 = log.(fund_full_data.net_assets_m1)
    fund_full_data.log_age = log.(fund_full_data.age)
    
    sort!(fund_full_data, [:fundid, :date])
    select!(fund_full_data, Not(["inception_date", "age", "net_assets_m1"]))

    return fund_full_data
end

function _prepare_factors(factors_data, model) 
    model_source = model[1]
    model_factors = model[2]

    source_condition = (
        factors_data.source_id .== model_source .||
        factors_data.source_id .== "fx"
    )

    factor_condition = in.(factors_data.factor, Ref(String.(model_factors)))

    source_factors = factors_data[source_condition .&& factor_condition, :]

    wide_factors = unstack(source_factors, :date, :factor, :ret)
    dropmissing!(wide_factors)

    return wide_factors
end

function printtime(
        task, start_time;
        process_start_time=nothing, minutes=false
        )

    duration_s = round(time() - start_time, digits=2)
    duration_m = round(duration_s / 60, digits=2)
    
    if isnothing(process_start_time)
        printout = "Finished $task in $duration_s seconds"
        minutes && (printout *= " ($duration_m minutes)")
    else
        process_duration_s = round(time() - process_start_time, digits=2)
        process_duration_m = round(process_duration_s / 60, digits=2)

        printout = "Finished $task in $process_duration_s seconds"
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


function datastep(stepper; reset=false, freeze=false)
    """
    datastep(stepper; reset=false, freeze=false)

    Returns the next value in the stepper iterator and the corresponding data from the stepper
    data. If the stepper is empty, nothing is returned.

    Parameters
    ----------
    stepper : NamedTuple
        The stepper object containing the iterator and data.
    reset : Bool
        If true, the stepper iterator is reset to the first value.
    freeze : Bool
        If true, the stepper iterator is not advanced to the next value.
    """

    if reset
        Iterators.reset!(stepper.itr)
    end
    if freeze
        next_state_following_index = stepper.itr.nextvalstate[ITR_NEXT_INDEX]
        current_state_prev_index = next_state_following_index - 2
        current_state_prev_index == 0 && error("Cannot freeze stepper at first value.")
        value_i = stepper.itr.itr[current_state_prev_index]
    else
        value_i = iterate(stepper.itr)[ITR_VALUE]
    end

    value_data = stepper.data[nonmissing(stepper.data[:, stepper.field] .== value_i), :]

    return value_data
end

function datastepper(data, field, values)
    stepper = (
        itr = Iterators.Stateful(values),
        data = data,
        field = field
    )

    return stepper
end

stepclass(data, ids) = datastepper(data, :fund_class_id, ids)
stepgroup(data, ids) = datastepper(data, :class_group_id, ids)
stepfund(data, ids) = datastepper(data, :fund_id, ids)

drop_allmissing!(df; dims=1) = drop_allmissing!(df, propertynames(df); dims=dims)
function drop_allmissing!(df, cols; dims=1)
    if dims ∉ [1, 2, :row, :rows, :col, :cols]
        error("dims must be :rows or :cols")
    end

    dimsmap = Dict(:row => 1, :rows => 1, :col => 2, :cols => 2)
    if dims ∉ [1, 2]
        dims = dimsmap[dims]
    end

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

function filter_fundids(condition, data)
    info_filename = joinpath(DIRS.mf.init, "mf-info.arrow")
    info = loadarrow(info_filename)
    select!(info, [:fundid, :global_category, :morningstar_category, :us_category_group, :investment_area])

    _assert_similar_fundids(info)
    info = unique(info, :fundid)

    joined_data = innerjoin(data, info, on=:fundid)
    filtered_data = joined_data[condition(joined_data), propertynames(data)]

    return filtered_data
end

function investment_target_is(data, target)
    if target ∉ [:usa, :wld, :emg]
        error("Invalid target: $target. Must be :usa, :wld, or :emg.")
    end

    if target == :usa
        condition = (
            (.!ismissing.(data.us_category_group) .&& (data.us_category_group .== "US Equity")) .||
            (.!ismissing.(data.investment_area) .&& (data.investment_area .== "United States of America"))
        )
    elseif target == :wld
        condition = (
            (.!ismissing.(data.us_category_group) .&& (data.us_category_group .== "International Equity")) .||
            (.!ismissing.(data.investment_area) .&& (data.investment_area .!= "United States of America"))
        )
    elseif target == :emg
        condition = (
            data.morningstar_category .== "US Fund Diversified Emerging Mkts" .||
            (.!ismissing.(data.investment_area) .&& (data.investment_area .== "Global Emerging Mkts"))
        )
    end

    return condition
end

bho_dates_only(data) = (data.date .>= Date(1996,1,1)) .&& (data.date .<= Date(2011,11,1))
post_bho_only(data) = data.date .> Date(2011,11,1)

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
        date_cols = [start_date + Month(i) for i in 0:n_date_cols-1]

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

function _assert_similar_fundids(info)
    fundids = unique(info.fundid)
    test_fields = setdiff(propertynames(info), [:fundid])

    for fund in fundids
        for field in test_fields
            if length(unique(info[info.fundid .== fund, field])) > 1
                error("Non-unique $field for fundid $fund.")
            end
        end
    end
    
    return
end

end # module CommonFunctions
