function debug_record(tag, timestamp, record)
    print("--- RECORD DEBUG START ---")
    for k,v in pairs(record) do
        print("KEY:", k, "TYPE:", type(v))
        if type(v) == "table" then
            for kk, vv in pairs(v) do
                print("   SUBKEY:", kk, "TYPE:", type(vv))
            end
        end
    end
    print("--- RECORD DEBUG END ---")
    return 1, record
end

-- Function to convert binary data to hex string
function binary_to_hex(binary_data)
    if type(binary_data) ~= "string" then
        return binary_data
    end
    
    local hex_string = ""
    for i = 1, #binary_data do
        hex_string = hex_string .. string.format("%02x", string.byte(binary_data, i))
    end
    return hex_string
end

-- Function to add trace_type field based on log patterns AND OTLP metadata
function add_trace_type(tag, timestamp, record)
    -- Check both log message patterns and OTLP metadata to identify traces
    local log_message = record.log or ""
    
    -- Add debug info
    record.debug_log_message = log_message
    record.debug_log_length = string.len(log_message)
    
    -- Patterns that typically indicate trace data
    local trace_patterns = {
        "Order details:",
        "Calculated quote",
        "Consumed record with orderId:",
        "@OrderResult",
        "trace_id",
        "span_id",
        "Targeted ad request received",
        "Non-targeted ad request received",
        "preparing random response",
        "no baggage found in context",
        "GetAds called",
        "GetCart called",
        "GetProduct called",
        "PlaceOrder called",
        "GetQuote called",
        "ShipOrder called",
        "SendOrderConfirmation called",
        "ChargeServiceRequest",
        "GetRecommendations called"
    }
    
    -- Check if log message contains trace-related patterns
    local is_trace = false
    local matched_pattern = ""
    for _, pattern in ipairs(trace_patterns) do
        if string.find(log_message, pattern, 1, true) then
            is_trace = true
            matched_pattern = pattern
            break
        end
    end
    
    -- Note: OTLP metadata fields are not available in Lua filter context
    -- They are added to the record structure after Lua processing
    -- So we rely on pattern matching for trace detection
    
    -- Add debug info about pattern matching
    record.debug_matched_pattern = matched_pattern
    record.debug_is_trace = tostring(is_trace)
    
    -- Set trace_type based on pattern matching
    if is_trace then
        record.trace_type = "trace"
    else
        record.trace_type = "log"
    end
    
    return 1, timestamp, record
end

-- Simple function to convert only the two specific binary fields
function convert_otel_ids(tag, timestamp, record)
    local converted = false
    
    -- Handle __internal___log_metadata_otlp_trace_id at top level
    if record["__internal___log_metadata_otlp_trace_id"] and type(record["__internal___log_metadata_otlp_trace_id"]) == "string" then
        local trace_id_hex = binary_to_hex(record["__internal___log_metadata_otlp_trace_id"])
        print("[FILTER] Converting top-level trace_id: " .. trace_id_hex)
        record["__internal___log_metadata_otlp_trace_id"] = trace_id_hex
        converted = true
    end
    
    -- Handle __internal___log_metadata_otlp_span_id at top level
    if record["__internal___log_metadata_otlp_span_id"] and type(record["__internal___log_metadata_otlp_span_id"]) == "string" then
        local span_id_hex = binary_to_hex(record["__internal___log_metadata_otlp_span_id"])
        print("[FILTER] Converting top-level span_id: " .. span_id_hex)
        record["__internal___log_metadata_otlp_span_id"] = span_id_hex
        converted = true
    end
    
    -- Handle nested otlp structure
    if record["otlp"] and type(record["otlp"]) == "table" then
        -- Handle trace_id inside otlp
        if record["otlp"]["trace_id"] and type(record["otlp"]["trace_id"]) == "string" then
            local trace_id_hex = binary_to_hex(record["otlp"]["trace_id"])
            print("[FILTER] Converting nested otlp trace_id: " .. trace_id_hex)
            record["otlp"]["trace_id"] = trace_id_hex
            converted = true
        end
        
        -- Handle span_id inside otlp
        if record["otlp"]["span_id"] and type(record["otlp"]["span_id"]) == "string" then
            local span_id_hex = binary_to_hex(record["otlp"]["span_id"])
            print("[FILTER] Converting nested otlp span_id: " .. span_id_hex)
            record["otlp"]["span_id"] = span_id_hex
            converted = true
        end
    end
    
    if converted then
        print("[FILTER] Record processed with conversions")
    end
    
    -- Return: code (1=keep, 0=drop, -1=error), timestamp, record
    return 1, timestamp, record
end
