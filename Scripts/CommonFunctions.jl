module CommonFunctions

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

export push_with_currency_code!
export option_foldername

end