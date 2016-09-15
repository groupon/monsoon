package com.groupon.lex.metrics.transformers;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.MutableContext;
import com.groupon.lex.metrics.transformers.IdentifierNameResolver.SubSelectIndex;
import com.groupon.lex.metrics.transformers.IdentifierNameResolver.SubSelectRange;
import static java.util.Collections.EMPTY_MAP;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class IdentifierNameResolverTest {
    private final SimpleGroupPath group_path = SimpleGroupPath.valueOf("foo", "bar");
    private final static String IDENTIFIER = "ident";
    TimeSeriesValue tsv0;
    private Context ctx;

    @Before
    public void setup() {
        final GroupName group_name = GroupName.valueOf(group_path);
        final TimeSeriesCollectionPair ts_data = new TimeSeriesCollectionPairInstance(DateTime.now(DateTimeZone.UTC));
        tsv0 = new MutableTimeSeriesValue(ts_data.getCurrentCollection().getTimestamp(), group_name, EMPTY_MAP);
        ts_data.getCurrentCollection().add(tsv0);

        final MutableContext m_ctx = new MutableContext(ts_data, (alert) -> {});
        m_ctx.putGroupAlias(IDENTIFIER, group_path);

        ctx = m_ctx;
    }

    @Test
    public void resolve() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER);

        assertEquals(group_path, resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_index() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectIndex(1)));

        assertEquals(SimpleGroupPath.valueOf("bar"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_negative_index() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectIndex(-1)));

        assertEquals(SimpleGroupPath.valueOf("bar"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_b_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(0), Optional.of(1))));

        assertEquals(SimpleGroupPath.valueOf("foo"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_neg_b_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(-1), Optional.of(2))));

        assertEquals(SimpleGroupPath.valueOf("bar"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_b_neg_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(0), Optional.of(-1))));

        assertEquals(SimpleGroupPath.valueOf("foo"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_neg_b_neg_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(-2), Optional.of(-1))));

        assertEquals(SimpleGroupPath.valueOf("foo"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_absent_b_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.empty(), Optional.of(1))));

        assertEquals(SimpleGroupPath.valueOf("foo"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_b_absent_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(-1), Optional.empty())));

        assertEquals(SimpleGroupPath.valueOf("bar"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void resolve_by_range_absent_b_absent_e() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.empty(), Optional.empty())));

        assertEquals(SimpleGroupPath.valueOf("foo", "bar"), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void too_high_index() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectIndex(3)));

        assertEquals(SimpleGroupPath.valueOf(), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test
    public void too_high_range() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.empty(), Optional.of(3))));

        assertEquals(SimpleGroupPath.valueOf(), resolver.apply(ctx).map(p -> SimpleGroupPath.valueOf(p.getPath())).get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalid_range() {
        final IdentifierNameResolver resolver = new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(1), Optional.of(0))));

        resolver.apply(ctx);
    }

    @Test
    public void config_string() {
        assertEquals("${ident}", new IdentifierNameResolver(IDENTIFIER)
                .configString().toString());
        assertEquals("${ident[0]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectIndex(0)))
                .configString().toString());
        assertEquals("${ident[-1]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectIndex(-1)))
                .configString().toString());
        assertEquals("${ident}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.empty(), Optional.empty())))
                .configString().toString());
        assertEquals("${ident[-1:]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(-1), Optional.empty())))
                .configString().toString());
        assertEquals("${ident[1:]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(1), Optional.empty())))
                .configString().toString());
        assertEquals("${ident[:-1]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.empty(), Optional.of(-1))))
                .configString().toString());
        assertEquals("${ident[:2]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.empty(), Optional.of(2))))
                .configString().toString());
        assertEquals("${ident[-2:-1]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(-2), Optional.of(-1))))
                .configString().toString());
        assertEquals("${ident[1:2]}", new IdentifierNameResolver(IDENTIFIER, Optional.of(new SubSelectRange(Optional.of(1), Optional.of(2))))
                .configString().toString());
    }
}
