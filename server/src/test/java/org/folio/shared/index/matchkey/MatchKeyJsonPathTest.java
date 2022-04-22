package org.folio.shared.index.matchkey;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.Set;
import org.folio.shared.index.matchkey.impl.MatchKeyJsonPath;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class MatchKeyJsonPathTest {

  @Test
  public void matchKeyJsonPathNonConfigured() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    JsonObject payload = new JsonObject();
    Set<String> keys = new HashSet<>();
    Exception e = Assert.assertThrows(
        MatchKeyException.class,
        () -> matchKeyMethod.getKeys(payload, payload, keys));
    assertThat(e.getMessage(), is("Not configured"));
  }

  @Test
  public void matchKeyJsonPathConfigureBad() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    JsonObject jsonObject = new JsonObject();
    Exception e = Assert.assertThrows(
        MatchKeyException.class,
        () ->     matchKeyMethod.configure(jsonObject));
    assertThat(e.getMessage(), is("jsonpath: either \"marc\" or \"inventory\" must be given"));
  }

  @Test
  public void matchKeyJsonPathConfigureInvalidJsonPath() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    JsonObject configuration = new JsonObject().put("marc", "$.fields.010.subfields[x");
    Assert.assertThrows(InvalidPathException.class,
        () -> matchKeyMethod.configure(configuration));
  }

  @Test
  public void matchKeyJsonPathConfigureMarc() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    matchKeyMethod.configure(new JsonObject().put("marc", "$.fields.010.subfields[*].a"));

    JsonObject marc = new JsonObject()
        .put("leader", "00942nam  22002531a 4504")
        .put("fields", new JsonObject()
            .put("001", "   73209622 //r823")
            .put("010", new JsonObject()
                .put("subfields", new JsonArray()
                    .add(new JsonObject().put("b", "73209622"))
                )
            )
            .put("245", new JsonObject()
                .put("subfields", new JsonArray()
                    .add(new JsonObject().put("a", "The Computer Bible /"))
                    .add(new JsonObject().put("c", "J. Arthur Baird, David Noel Freedman, editors." ))
                )
            )
        );
    Set<String> keys = new HashSet<>();
    matchKeyMethod.getKeys(marc, new JsonObject(), keys);
    assertThat(keys, is(empty()));

    marc = new JsonObject()
        .put("leader", "00942nam  22002531a 4504")
        .put("fields", new JsonObject()
            .put("001", "   73209622 //r823")
            .put("010", new JsonObject()
                .put("subfields", new JsonArray()
                    .add(new JsonObject().put("a", "73209622"))
                    .add(new JsonObject().put("a", "73209623"))
                )
            )
            .put("245", new JsonObject()
                .put("subfields", new JsonArray()
                    .add(new JsonObject().put("a", "The Computer Bible /"))
                    .add(new JsonObject().put("c", "J. Arthur Baird, David Noel Freedman, editors." ))
                )
            )
        );
    keys.clear();
    matchKeyMethod.getKeys(marc, new JsonObject(), keys);
    assertThat(keys, containsInAnyOrder("73209622", "73209623"));
  }

  @Test
  public void matchKeyJsonPathConfigureInventory() {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    matchKeyMethod.configure(new JsonObject().put("inventory", "$.inventory.isbn[*]"));
    JsonObject inventory = new JsonObject()
        .put("inventory", new JsonObject()
            .put("isbn", new JsonArray().add("73209622")));
    Set<String> keys = new HashSet<>();
    matchKeyMethod.getKeys(new JsonObject(), inventory, keys);
    assertThat(keys, contains("73209622"));

    inventory = new JsonObject()
        .put("inventory", new JsonObject()
            .put("issn", new JsonArray().add("73209622")));
    keys.clear();
    matchKeyMethod.getKeys(new JsonObject(), inventory, keys);
    assertThat(keys, is(empty()));
  }

  void matchKeyVerify(String pattern, Set<String> expectedKeys, JsonObject inventoryPayload) {
    MatchKeyMethod matchKeyMethod = new MatchKeyJsonPath();
    matchKeyMethod.configure(new JsonObject().put("inventory", pattern));
    Set<String> keys = new HashSet<>();
    matchKeyMethod.getKeys(new JsonObject(), inventoryPayload, keys);
    Assert.assertEquals(expectedKeys, keys);
  }

  @Test
  public void matchKeyJsonPathExpressions() {
    JsonObject inventory = new JsonObject()
        .put("identifiers", new JsonArray()
            .add(new JsonObject()
                .put("isbn", "73209629"))
            .add(new JsonObject()
                .put("isbn", "73209623"))

        )
        .put("matchKey", new JsonObject()
            .put("title", "Panisci fistula")
            .put("remainder-of-title", " : tre preludi per tre flauti")
            .put("medium", "[sound recording]")
        )
        ;

    matchKeyVerify("$.identifiers[*].isbn", Set.of("73209629", "73209623"), inventory);
    matchKeyVerify("$.matchKey.title", Set.of("Panisci fistula"), inventory);
    matchKeyVerify("$.matchKey", Set.of(), inventory);
    matchKeyVerify("$.matchKey[?(@.title)]", Set.of(), inventory);
  }

}
