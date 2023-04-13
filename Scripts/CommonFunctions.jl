module CommonFunctions

function push_with_currency_code!(datalist, df, currency_code)
    append_data = copy(df)
    append_data.cur_code .= currency_code
    append_data = append_data[:, [:cur_code, :date, :mkt]]
    push!(datalist, append_data)
end

export push_with_currency_code!

end