package net.quasardb.kafka.common;

import java.util.HashMap;
import java.util.Map;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.exception.AliasNotFoundException;
import net.quasardb.qdb.ts.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry that contains one or more TableInfo entries, which can be
 * used to lookup and cache metadata about a Table's representation.
 */
public class TableRegistry {
    private static final Logger log = LoggerFactory.getLogger(TableRegistry.class);
    private Map<String, TableInfo> registry;

    public TableRegistry() {
        this.registry = new HashMap<>();
    }

    /**
     * Dynamically resolve a table and add it to the registry. Blocking
     * function.
     *
     * @param session Active connection with the QuasarDB cluster
     * @param name Table name to look up and add.
     * @return A reference to the added TableInfo object inside the registry, or
     *         null when the table was not found.
     */
    public TableInfo put(Session session, String name) {
        try {
            return this.put(name, new Table(session, name));
        } catch (AliasNotFoundException e) {
            return null;
        }
    }

    public TableInfo put(Table t) {
        return this.put(t.getName(), t);
    }

    public TableInfo put(String name, Table t) {
        return this.put(name, new TableInfo(t));
    }

    public TableInfo put(String name, TableInfo t) {
        this.registry.put(name, t);

        log.debug("Added table {} to TableRegistry", name);
        return t;
    }

    /**
     * Retrieve a Table by its name, or return null when not found.
     *
     * @param name Name of the table to look up
     * @return Reference to the Tableinfo
     */
    public TableInfo get(String name) {
        return this.registry.get(name);
    }
};
