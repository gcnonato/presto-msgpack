/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.channelmeter.presto.msgpack;

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.type.SqlType;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class MsgPackFunctions
{
    private static final ObjectMapper MSGPACK_FACTORY = new ObjectMapper(new MessagePackFactory());

    private MsgPackFunctions() {}

    @ScalarFunction("msgpack_extract_map")
    @Nullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice varcharMsgpackExtract(@SqlType(StandardTypes.VARBINARY) Slice msg, @SqlType(StandardTypes.VARCHAR) Slice key)
    {
        try {
           TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>(){};
            Map<String, Object> xs = MSGPACK_FACTORY.readValue(msg.getInput(), typeReference);
            String mapKey = key.toString(UTF_8);
            if (xs.get(mapKey) == null) {
                return null;
            }
            return Slices.wrappedBuffer(xs.get(mapKey).toString().getBytes(UTF_8));
        } catch (IOException e) {
            return null;
        }
    }

    @ScalarFunction("msgpack_extract_map_i")
    @SqlType(StandardTypes.BIGINT)
    public static long intMsgpackExtract(@SqlType(StandardTypes.VARBINARY) Slice msg, @SqlType(StandardTypes.VARCHAR) Slice key)
    {
        try {
            TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>(){};
            Map<String, Object> xs = MSGPACK_FACTORY.readValue(msg.getInput(), typeReference);
            String mapKey = key.toString(UTF_8);
            if (xs.get(mapKey) == null) {
                return 0;
            }
            if (xs.get(mapKey) instanceof Number) {
                return ((Number)xs.get(mapKey)).longValue();
            }
            return 0;
        } catch (IOException e) {
            return 0;
        }
    }

    @ScalarFunction("msgpack_extract_map_f")
    @SqlType(StandardTypes.DOUBLE)
    public static double floatMsgpackExtract(@SqlType(StandardTypes.VARBINARY) Slice msg, @SqlType(StandardTypes.VARCHAR) Slice key)
    {
        try {
            TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>(){};
            Map<String, Object> xs = MSGPACK_FACTORY.readValue(msg.getInput(), typeReference);
            String mapKey = key.toString(UTF_8);
            if (xs.get(mapKey) == null) {
                return 0;
            }
            if (xs.get(mapKey) instanceof Number) {
                return ((Number)xs.get(mapKey)).doubleValue();
            }
            return 0;
        } catch (IOException e) {
            return 0;
        }
    }

}
