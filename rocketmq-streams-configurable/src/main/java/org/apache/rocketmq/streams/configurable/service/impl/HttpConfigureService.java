package org.apache.rocketmq.streams.configurable.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HTTP;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.Args;
import org.apache.http.util.CharArrayBuffer;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.apache.rocketmq.streams.configurable.model.Configure;
import org.apache.rocketmq.streams.configurable.service.AbstractConfigurableService;


public class HttpConfigureService extends AbstractConfigurableService {

    public static final int NORMAL_STATUES = 200;
    public static final String CHARSET = "UTF-8";
    public static final int TIMOUT = 10000;
    public static final int CONNECT_TIMOUT = 10000;


    protected String accessId;
    protected String accessIdSecret;
    protected String endPoint;

    protected transient CloseableHttpClient client;

    private static final Log LOG = LogFactory.getLog(HttpConfigureService.class);

    public static void main(String[] args) {
        HttpConfigureService service = new HttpConfigureService("", "", "http://11.158.168.161:8888/queryConfigure");
        service.loadConfigurable("test");
    }

    public HttpConfigureService() {
        init();
    }

    public HttpConfigureService(String accessId, String accessIdSecret, String endPoint) {
        this.accessId = accessId;
        this.accessIdSecret = accessIdSecret;
        this.endPoint = endPoint;
        init();
    }

    public HttpConfigureService(Properties properties) {
        super(properties);
        this.accessId = properties.getProperty(AbstractComponent.HTTP_AK);
        this.accessIdSecret = properties.getProperty(AbstractComponent.HTTP_SK);
        this.endPoint = properties.getProperty(HTTP_SERVICE_ENDPOINT) + "/queryConfigure";
        init();
    }

    public void init() {
        RequestConfig.Builder configBuilder = RequestConfig.custom();
        configBuilder.setConnectionRequestTimeout(CONNECT_TIMOUT);
        configBuilder.setConnectTimeout(CONNECT_TIMOUT);
        configBuilder.setSocketTimeout(TIMOUT);
        SSLConnectionSocketFactory sslsf = null;
        try {
            SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
            sslsf = new SSLConnectionSocketFactory(sslcontext, new HostnameVerifier() {
                @Override
                public boolean verify(String s, SSLSession sslSession) {
                    return true;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ConnectionConfig connectionConfig = ConnectionConfig.custom().setCharset(Consts.UTF_8).build();
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register("https",
            sslsf).register("http",
            new PlainConnectionSocketFactory()).build();
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        connManager.setDefaultConnectionConfig(connectionConfig);
        connManager.setMaxTotal(500);
        connManager.setDefaultMaxPerRoute(50);
        HttpClientBuilder clientBuilder = HttpClients.custom();

        clientBuilder.setDefaultRequestConfig(configBuilder.build());
        clientBuilder.setSSLSocketFactory(sslsf);
        clientBuilder.setConnectionManager(connManager);

        client = clientBuilder.build();

    }

    @Override
    public GetConfigureResult loadConfigurable(String namespace) {
        GetConfigureResult result = new GetConfigureResult();
//        request.setDipperNamespace(namespace);
        try {
            JSONObject param = new JSONObject();
            param.put("namespace", namespace);
            CloseableHttpResponse response = post(endPoint, param.toJSONString(), null);
            if (response == null ) {
                result.setQuerySuccess(false);
                if (LOG.isErrorEnabled()) {
                    LOG.error("loadConfigurable error!namespace=" + namespace + ",response=" + JSONObject.toJSONString(
                        response));
                }
            } else {
                result.setQuerySuccess(true);
                List<Configure> configures = new ArrayList<Configure>();
                String content = toString(response.getEntity(), Charset.forName(CHARSET));
                JSONObject object = JSONObject.parseObject(content);
                JSONArray data = object.getJSONArray("data");
                System.out.println(data.size());
                configures = convert2Configure(data);
                List<IConfigurable> configurables = convert(configures);
                result.setConfigurables(configurables);
                System.out.println(configures.size() + "    " + configurables.size());
//                Data data = response.getEntity().getContent()
//                if (data != null) {
//                    for (Item item : data.getItems()) {
//                        Configure configure = convert2Configure(item);
//                        configures.add(configure);
//                    }
//                }
//                result.setConfigurables(convert(configures));
            }
        } catch (Exception e) {
            result.setQuerySuccess(false);
            if (LOG.isErrorEnabled()) {
                LOG.error("loadConfigurable error!namespace=" + namespace, e);
            }
        }
        return result;
    }


    public  CloseableHttpResponse get(String url, Map<String, String> param) {

        try {
            HttpGet httpGet = new HttpGet(url);
            if (param != null && param.size() > 0) {
                List<Header> headers = new ArrayList<>();
                for (Map.Entry<String, String> tmp : param.entrySet()) {
                    httpGet.addHeader(tmp.getKey(), tmp.getValue());
                }
            }
            CloseableHttpResponse response = client.execute(httpGet);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    public  CloseableHttpResponse get(String url, Header... headers) {
        try {
            HttpGet httpGet = new HttpGet(url);
            if (headers != null && headers.length > 0) {
                for (Header header : headers) {
                    httpGet.addHeader(header);
                }
            }
            CloseableHttpResponse response = client.execute(httpGet);
            return response;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public  CloseableHttpResponse post(String url, String body, Header... headers) {
        try {
            HttpPost httpPost = new HttpPost(url);
            StringEntity stringEntity = new StringEntity(body, CHARSET);
            stringEntity.setContentEncoding(CHARSET);
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);
            if (headers != null && headers.length > 0) {
                for (Header header : headers) {
                    httpPost.addHeader(header);
                }
            }
            CloseableHttpResponse response = client.execute(httpPost);
            return response;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    protected List<Configure> convert2Configure(JSONArray array) {
        List<Configure> configures = new ArrayList<Configure>();
        for (int i = 0; i < array.size(); i++) {
            Configure configure = array.getObject(i, Configure.class);
            if (!namespace.equalsIgnoreCase(configure.getNameSpace())) {
                continue;
            }
            try {
                configure.setJsonValue(AESUtil.aesDecrypt(configure.getJsonValue(),
                    ComponentCreator.getProperties().getProperty(ConfigureFileKey.SECRECY, ConfigureFileKey.SECRECY_DEFAULT)));
            } catch (Exception e) {
                e.printStackTrace();
            }
            configures.add(configure);
        }
        return configures;
    }

    protected List<IConfigurable> convert(List<Configure> configures) {
        if (configures == null) {
            return new ArrayList<IConfigurable>();
        }
        List<IConfigurable> configurables = new ArrayList<IConfigurable>();
        for (Configure configure : configures) {
            IConfigurable configurable = convert(configure);
            if (configurable != null) {
                configurables.add(configurable);
            }
        }
        return configurables;
    }

//    private Configure convert2Configure(Item item) {
//        Configure configure = new Configure();
//        configure.setAccountId(item.getAccountId());
//        configure.setAccountName(item.getAccountName());
//        configure.setAccountNickName(item.getAccountNickName());
//        configure.setClientIp(item.getDipperClientIp());
//        configure.setJsonValue(item.getJsonValue());
//        configure.setName(item.getName());
//        configure.setNameSpace(item.getDipperNamespace());
//        configure.setRequestId(item.getDipperRequestId());
//        configure.setType(item.getType());
//        return configure;
//    }


    public  String toString(HttpEntity entity, Charset defaultCharset) throws IOException, ParseException {
        Args.notNull(entity, "Entity");
        InputStream instream = entity.getContent();
        if (instream == null) {
            return null;
        } else {
            try {
                Args.check(entity.getContentLength() <= 2147483647L, "HTTP entity too large to be buffered in memory");
                int i = (int)entity.getContentLength();
                if (i < 0) {
                    i = 4096;
                }

                Charset charset = null;

                try {
                    ContentType contentType = ContentType.get(entity);
                    if (contentType != null) {
                        charset = contentType.getCharset();
                    }
                } catch (UnsupportedCharsetException var13) {
                    if (defaultCharset == null) {
                        throw new UnsupportedEncodingException(var13.getMessage());
                    }
                }

                if (charset == null) {
                    charset = defaultCharset;
                }

                if (charset == null) {
                    charset = HTTP.DEF_CONTENT_CHARSET;
                }

                Reader reader = new InputStreamReader(instream, charset);
                CharArrayBuffer buffer = new CharArrayBuffer(i);
                char[] tmp = new char[1024];

                int l;
                while((l = reader.read(tmp)) != -1) {
                    buffer.append(tmp, 0, l);
                }

                String var9 = buffer.toString();
                return var9;
            } finally {
                instream.close();
            }
        }
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessIdSecret() {
        return accessIdSecret;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public void setAccessIdSecret(String accessIdSecret) {
        this.accessIdSecret = accessIdSecret;
    }

    @Override
    public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {
        return null;
    }

    @Override
    protected void updateConfigurable(IConfigurable configurable) {

    }

    @Override
    protected void insertConfigurable(IConfigurable configurable) {

    }
}
