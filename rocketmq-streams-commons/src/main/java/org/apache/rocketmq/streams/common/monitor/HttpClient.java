package org.apache.rocketmq.streams.common.monitor;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

public class HttpClient {

    public static final int NORMAL_STATUES = 200;
    public static final String CHARSET = "UTF-8";
    public static final int TIMOUT = 10000;
    public static final int CONNECT_TIMOUT = 10000;

    protected String accessId;
    protected String accessIdSecret;
    protected String endPoint;

    protected transient CloseableHttpClient client;

    public HttpClient(String accessId, String accessIdSecret, String endPoint) {
        this.accessId = accessId;
        this.accessIdSecret = accessIdSecret;
        this.endPoint = endPoint;
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

    public CloseableHttpResponse get(String url, Header... headers) {
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
}
