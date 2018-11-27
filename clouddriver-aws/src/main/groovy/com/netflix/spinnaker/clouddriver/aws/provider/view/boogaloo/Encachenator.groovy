package com.netflix.spinnaker.clouddriver.aws.provider.view.boogaloo

import com.google.common.hash.Hashing
import com.netflix.spinnaker.clouddriver.aws.security.EddaTemplater
import com.netflix.spinnaker.clouddriver.aws.security.NetflixAmazonCredentials
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider
import com.netflix.spinnaker.kork.jedis.RedisClientDelegate
import groovy.util.logging.Slf4j
import org.apache.commons.codec.binary.Hex
import redis.clients.jedis.BinaryJedisCommands

import java.nio.charset.Charset
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@Slf4j
class Encachenator implements Closeable {

    final List<String> collections = [
            'aws/alarms',
            'aws/autoScalingGroups',
            'aws/images',
            'aws/launchConfigurations',
            'aws/loadBalancers',
            'aws/scalingPolicies',
            'aws/scheduledActions',
            'aws/subnets',
            'aws/targetGroups',
            'view/instances',
            'view/loadBalancerInstances',
            'view/targetGroupHealth'
    ]

    ScheduledExecutorService sched

    public Encachenator(AccountCredentialsProvider acp, RedisClientDelegate redisClientDelegate) {
        sched = Executors.newScheduledThreadPool(10)
        acp.all.findAll { it instanceof NetflixAmazonCredentials && it.eddaEnabled }.each { NetflixAmazonCredentials creds ->
            creds.regions.each { region ->
                collections.each { collection ->
                    sched.scheduleWithFixedDelay(new EncachenatorJob(region.name, creds, collection, redisClientDelegate), 15, 30, TimeUnit.SECONDS)
                }
            }
        }
    }

    @Override
    void close() throws IOException {
        sched.shutdown()

        if (!sched.awaitTermination(15, TimeUnit.SECONDS)) {
            sched.shutdownNow()
        }
    }
}

@Slf4j
class EncachenatorJob implements Runnable {
    final String region
    final NetflixAmazonCredentials creds
    final String eddaCollection
    final RedisClientDelegate redisClientDelegate
    final String key
    final byte[] redisKey
    final byte[] redisHashKey
    final URI uri

    EncachenatorJob(String region, NetflixAmazonCredentials creds, String eddaCollection, RedisClientDelegate redisClientDelegate) {
        this.region = region
        this.creds = creds
        this.eddaCollection = eddaCollection
        this.redisClientDelegate = redisClientDelegate
        key = "$creds.name/$region/$eddaCollection"
        redisKey = key.getBytes(Charset.forName('US-ASCII'))
        redisHashKey = "HASH:$key".getBytes(Charset.forName('US-ASCII'))
        uri = URI.create(EddaTemplater.defaultTemplater().getUrl(creds.edda, region) + '/REST/v2/' + eddaCollection + ';_expand').normalize()
    }

    private <T> T timeIt(GString msg, Closure<T> closure) {
        return timeIt(msg.toString(), closure)
    }
    private <T> T timeIt(String msg, Closure<T> closure) {
        long startTime = System.nanoTime()
        try {
            T rv = closure.call()
            log.info("$msg completed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}millis")
            return rv
        } catch (e) {
            log.info("$msg failed in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)}millis")
            throw e
        }
    }

    public void run() {
        try {
            def hash = Hashing.murmur3_128().newHasher()
            byte[] eddaData = timeIt("$key read edda data") {
                HttpURLConnection con = uri.toURL().openConnection()
                con.addRequestProperty("Accept-Encoding", "gzip")
                ByteArrayOutputStream baos = new ByteArrayOutputStream()
                byte[] buf = new byte[16 * 1024]
                int bytesRead
                con.getInputStream().withStream { is ->
                    while ((bytesRead = is.read(buf)) != -1) {
                        baos.write(buf, 0, bytesRead)
                        hash.putBytes(buf, 0, bytesRead)
                    }
                }
                con.disconnect()
                return baos.toByteArray()
            }

            log.info("$key raw edda data is $eddaData.length bytes")

            timeIt("$key store edda data") {
                redisClientDelegate.withBinaryClient({ bc ->
                    def existingHash = bc.get(redisHashKey) ?: new byte[0]
                    def currentHash = hash.hash().asBytes()
                    if (!Arrays.equals(existingHash, currentHash)) {
                        log.info("$key hash mismatch, updating the data: existing ${Hex.encodeHex(existingHash)} current ${Hex.encodeHex(currentHash)}")
                        bc.set(redisHashKey, currentHash)
                        bc.set(redisKey, eddaData)
                    } else {
                        log.info("$key hash matches, skipping save")
                    }
                } as Consumer<BinaryJedisCommands>)
            }
        } catch (e) {
            log.warn("Failboat", e)
        }
    }

}
