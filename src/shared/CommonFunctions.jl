module CommonFunctions

using
    DataFrames,
    Base.Threads,
    Dates

export
    makepath,
    push_with_currency_code!,
    option_foldername,
    group_transform!,
    group_transform,
    group_combine,
    savecols,
    ismissing_or_blank,
    name_model,
    offset_monthend

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
    append_data.cur_code .= currency_code
    append_data = append_data[:, [:cur_code, :date, value_columns]]
    push!(datalist, append_data)
end

function option_foldername(; currency_type, kwargs...)
    if currency_type == "local"
        folder_name = "local-rets"
    elseif currency_type == "usd"
        folder_name = "usd-rets"
    else
        error("Invalid currency type: $(kwargs[:currency_type]). Must be 'local' or 'usd'.")
    end

    if haskey(kwargs, :raw_ret_only) && kwargs[:raw_ret_only] == false
        folder_name *= "_gret-filled"
    end

    if haskey(kwargs, :polation_method)
        if kwargs[:polation_method] == "both"
            folder_name *= "_na-int-exp"
        elseif kwargs[:polation_method] == "interpolate"
            folder_name *= "_na-int"
        elseif kwargs[:polation_method] == "extrapolate"
            folder_name *= "_na-exp"
        elseif kwargs != false
            error("Invalid polation method: $(kwargs[:polation_method]). Must be 'both', "*
                  "'interpolate', 'extrapolate', or false.")
        end
    end
    
    if haskey(kwargs, :strict_eq) && kwargs[:strict_eq] == true
        if haskey(kwargs, :exc_finre) && kwargs[:exc_finre] == true
            folder_name *= "_eq-strict-exfinre"
        else
            folder_name *= "_eq-strict"
        end
    elseif haskey(kwargs, :exc_finre) && kwargs[:exc_finre] == true
        folder_name *= "_eq-exfinre"
    end

    if haskey(kwargs, :inv_targets) && kwargs[:inv_targets] == true
        folder_name *= "_targets"
    end

    if haskey(kwargs, :age_filter) && kwargs[:age_filter] == true
        folder_name *= "_age-filtered"
    end

    return folder_name
end

function savecols()
    println(stacktrace(backtrace()))
end

@inline function group_transform!(df, group_cols, input_cols, f::Function, output_cols)
    groupby(df, group_cols) |> x -> transform!(x, input_cols => f => output_cols)
end

@inline function group_transform(df, group_cols, input_cols, f::Function, output_cols)
    output = groupby(df, group_cols) |> x -> transform(x, input_cols => f => output_cols)

    return output
end

@inline function group_combine(df, group_cols, input_cols, f::Function, output_cols;
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

@inline name_model(model) = "$(model[2])_$(model[1])"
@inline offset_monthend(date, offset=1) = date + Dates.Month(offset) |> Dates.lastdayofmonth
@inline ismissing_or_blank(x) = ismissing(x) || x == ""

end