/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.sts;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.Modifier;
import javassist.NotFoundException;


public class ClientTransformer implements ClassFileTransformer {

    final String name = "com.aliyun.openservices.log.Client.SendData(java.lang.String,com.aliyun.openservices.log.http.client.HttpMethod,java.lang.String,java.util.Map,java.util.Map,byte[],java.util.Map,java.lang.String)";
    final String methodBody = "\n" +
        "\n" +
        "final boolean isDipperRefresh = (headers.get(\"dipper_sts_refresh\") != null);\n" +
        "if(isDipperRefresh){\n" +
        "\theader.remove(\"dipper_sts_refresh\");\n" +
        "}\n" +
        "if (body.length > 0) {\n" +
        "\theaders.put(Consts.CONST_CONTENT_MD5, DigestUtils.md5Crypt(body));\n" +
        "}\n" +
        "if (resourceOwnerAccount != null && !resourceOwnerAccount.isEmpty()) {\n" +
        "\theaders.put(Consts.CONST_X_LOG_RESOURCEOWNERACCOUNT, resourceOwnerAccount);\n" +
        "}\n" +
        "headers.put(Consts.CONST_CONTENT_LENGTH, String.valueOf(body.length));\n" +
        "DigestUtils.addSignature(credentials, method.toString(), headers, resourceUri, parameters);\n" +
        "URI uri;\n" +
        "if (serverIp == null) {\n" +
        "\turi = GetHostURI(project);\n" +
        "} else {\n" +
        "\turi = GetHostURIByIp(serverIp);\n" +
        "}\n" +
        "RequestMessage request = BuildRequest(uri, method,\n" +
        "\t\tresourceUri, parameters, headers,\n" +
        "\t\tnew ByteArrayInputStream(body), body.length);\n" +
        "ResponseMessage response = null;\n" +
        "try {\n" +
        "\tresponse = this.serviceClient.sendRequest(request, Consts.UTF_8_ENCODING);\n" +
        "\tExtractResponseBody(response);\n" +
        "\tif (outputHeader != null) {\n" +
        "\t\toutputHeader.putAll(response.getHeaders());\n" +
        "\t}\n" +
        "\tint statusCode = response.getStatusCode();\n" +
        "\tif (statusCode != Consts.CONST_HTTP_OK) {\n" +
        "\t\tString requestId = GetRequestId(response.getHeaders());\n" +
        "\t\ttry {\n" +
        "\t\t\tJSONObject object = parseResponseBody(response, requestId);\n" +
        "\t\t\tif(object.getString(Consts.CONST_ERROR_CODE).equals(\"Unauthorized\") && !isDipperRefresh){\n" +
        "\t\t\t\tStsIdentity stsIdentity = stsService.getStsIdentity();\n" +
        "\t\t\t\tcredentials.setAccessKeyId(stsIdentity.getAccessKeyId());\n" +
        "\t\t\t\tcredentials.setAccessKeySecret(stsIdentity.getAccessKeySecret());\n" +
        "\t\t\t\tcredentials.setSecurityToken(stsIdentity.getSecurityToken());\n" +
        "\t\t\t\theaders.put(\"dipper_sts_refresh\", \"1\");\n" +
        "\t\t\t\treturn  SendData(project, method, resourceUri, parameters, headers, body, outputHeader, serverIp)\n" +
        "\t\t\t}\n" +
        "\t\t\tErrorCheck(object, requestId, statusCode);\n" +
        "\t\t} catch (LogException ex) {\n" +
        "\t\t\tex.SetHttpCode(response.getStatusCode());\n" +
        "\t\t\tthrow ex;\n" +
        "\t\t}\n" +
        "\t}\n" +
        "} catch (ServiceException e) {\n" +
        "\tthrow new LogException(\"RequestError\", \"Web request failed: \" + e.getMessage(), e, \"\");\n" +
        "} catch (ClientException e) {\n" +
        "\tthrow new LogException(\"RequestError\", \"Web request failed: \" + e.getMessage(), e, \"\");\n" +
        "} finally {\n" +
        "\ttry {\n" +
        "\t\tif (response != null) {\n" +
        "\t\t\tresponse.close();\n" +
        "\t\t}\n" +
        "\t} catch (IOException ignore) {}\n" +
        "}\n" +
        "return response;";

    @Override
    public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
        if (className.contains("Client")) {
            final ClassPool classPool = ClassPool.getDefault();
            final CtClass clazz;
            try {
                clazz = classPool.get("com.aliyun.openservices.log.Client");
                CtField ctField = new CtField(classPool.get("org.apache.rocketmq.streams.sls.source.sts.StsService"), "stsService", clazz);
                ctField.setModifiers(Modifier.PUBLIC);
                clazz.addMethod(CtNewMethod.setter("setStsService", ctField));
                CtMethod[] convertToAbbr = clazz.getDeclaredMethods("SendData");
                for (CtMethod ctm : convertToAbbr) {
                    if (name.equals(ctm.getLongName())) {
                        ctm.setBody(this.methodBody);
                        break;
                    }
                }
                byte[] byteCode = clazz.toBytecode();
                //detach的意思是将内存中曾经被javassist加载过的Date对象移除，如果下次有需要在内存中找不到会重新走javassist加载
                clazz.detach();
                return byteCode;
            } catch (NotFoundException e) {
                e.printStackTrace();
            } catch (CannotCompileException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        // 如果返回null则字节码不会被修改
        return null;
    }
}
