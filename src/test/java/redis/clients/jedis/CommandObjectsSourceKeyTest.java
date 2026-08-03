package redis.clients.jedis;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.args.Rawable;
import redis.clients.jedis.params.GeoSearchParam;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.PrefixedKeyArgumentPreProcessor;
import redis.clients.jedis.util.SafeEncoder;

/**
 * Unit tests for the source key of the two commands that read one key and write another:
 * {@code ZRANGESTORE dst src ...} and {@code GEOSEARCHSTORE dst src ...}.
 * <p>
 * Both keys have to go through {@link CommandArguments#key(Object)}: that is what applies the
 * configured {@link CommandKeyArgumentPreProcessor} and records the key for hash slot computation.
 * A source key added as a plain argument reaches the server unprefixed and stays invisible to
 * cluster routing.
 */
public class CommandObjectsSourceKeyTest {

  private static final String DST = "dst";
  private static final String SRC = "src";
  private static final String PREFIX = "test-prefix:";

  /** Every overload that sends a source key, so no call site is left unguarded. */
  private static Map<String, CommandObject<?>> commandsWithSourceKey(CommandObjects co) {
    byte[] bdst = SafeEncoder.encode(DST);
    byte[] bsrc = SafeEncoder.encode(SRC);
    byte[] bmember = SafeEncoder.encode("member");
    GeoCoordinate coord = new GeoCoordinate(15, 37);

    Map<String, CommandObject<?>> commands = new LinkedHashMap<>();
    commands.put("zrangestore", co.zrangestore(DST, SRC, new ZRangeParams(0, -1)));
    commands.put("zrangestore binary", co.zrangestore(bdst, bsrc, new ZRangeParams(0, -1)));
    commands.put("geosearchStore fromMember byRadius",
      co.geosearchStore(DST, SRC, "member", 200, GeoUnit.KM));
    commands.put("geosearchStore fromMember byRadius binary",
      co.geosearchStore(bdst, bsrc, bmember, 200, GeoUnit.KM));
    commands.put("geosearchStore fromLonLat byRadius",
      co.geosearchStore(DST, SRC, coord, 200, GeoUnit.KM));
    commands.put("geosearchStore fromLonLat byRadius binary",
      co.geosearchStore(bdst, bsrc, coord, 200, GeoUnit.KM));
    commands.put("geosearchStore fromMember byBox",
      co.geosearchStore(DST, SRC, "member", 200, 100, GeoUnit.KM));
    commands.put("geosearchStore fromMember byBox binary",
      co.geosearchStore(bdst, bsrc, bmember, 200, 100, GeoUnit.KM));
    commands.put("geosearchStore fromLonLat byBox",
      co.geosearchStore(DST, SRC, coord, 200, 100, GeoUnit.KM));
    commands.put("geosearchStore fromLonLat byBox binary",
      co.geosearchStore(bdst, bsrc, coord, 200, 100, GeoUnit.KM));
    commands.put("geosearchStore params", co.geosearchStore(DST, SRC, geoSearchParam()));
    commands.put("geosearchStore params binary", co.geosearchStore(bdst, bsrc, geoSearchParam()));
    commands.put("geosearchStoreStoreDist params",
      co.geosearchStoreStoreDist(DST, SRC, geoSearchParam()));
    commands.put("geosearchStoreStoreDist params binary",
      co.geosearchStoreStoreDist(bdst, bsrc, geoSearchParam()));
    return commands;
  }

  private static GeoSearchParam geoSearchParam() {
    return GeoSearchParam.geoSearchParam().fromLonLat(15, 37).byRadius(200, GeoUnit.KM);
  }

  private static List<Integer> hashSlots(CommandObject<?> command) {
    List<Integer> slots = new ArrayList<>();
    for (Object key : command.getArguments().getKeys()) {
      slots.add(key instanceof byte[] ? JedisClusterCRC16.getSlot((byte[]) key)
          : JedisClusterCRC16.getSlot((String) key));
    }
    return slots;
  }

  private static String argument(CommandObject<?> command, int index) {
    int i = 0;
    for (Rawable raw : command.getArguments()) {
      if (i++ == index) return SafeEncoder.encode(raw.getRaw());
    }
    throw new IndexOutOfBoundsException("no argument at " + index);
  }

  @Test
  public void tracksSourceKeyForHashSlots() {
    List<Integer> expected = Arrays.asList(JedisClusterCRC16.getSlot(DST),
      JedisClusterCRC16.getSlot(SRC));

    // assertAll so one run names every overload that regressed, not just the first
    assertAll(assertions(commandsWithSourceKey(new CommandObjects(RedisProtocol.RESP2)),
      (name, command) -> assertEquals(expected, hashSlots(command),
        name + " must track the source key along with the destination key")));
  }

  @Test
  public void prefixesSourceKey() {
    CommandObjects commandObjects = new CommandObjects(RedisProtocol.RESP2);
    commandObjects.setKeyArgumentPreProcessor(new PrefixedKeyArgumentPreProcessor(PREFIX));

    assertAll(assertions(commandsWithSourceKey(commandObjects), (name, command) -> {
      assertEquals(PREFIX + DST, argument(command, 1),
        name + " must send the prefixed destination");
      assertEquals(PREFIX + SRC, argument(command, 2), name + " must send the prefixed source");
    }));
  }

  private static Stream<Executable> assertions(Map<String, CommandObject<?>> commands,
      BiConsumer<String, CommandObject<?>> assertion) {
    return commands.entrySet().stream()
        .map(entry -> () -> assertion.accept(entry.getKey(), entry.getValue()));
  }
}
