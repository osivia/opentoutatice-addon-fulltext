package org.opentoutatice.fulltext.reindex.query;

import org.apache.commons.lang.StringUtils;
import org.nuxeo.ecm.core.query.QueryFilter;
import org.nuxeo.ecm.core.storage.StorageException;
import org.nuxeo.ecm.core.storage.sql.Model;
import org.nuxeo.ecm.core.storage.sql.Session.PathResolver;
import org.nuxeo.ecm.core.storage.sql.jdbc.NXQLQueryMaker;
import org.nuxeo.ecm.core.storage.sql.jdbc.SQLInfo;
import org.nuxeo.ecm.core.storage.sql.jdbc.SQLInfo.SQLInfoSelect;
import org.nuxeo.ecm.core.storage.sql.jdbc.db.Table;

public class FulltextQueryMaker extends NXQLQueryMaker {

	public static final String TTC_FULLTEXT = "TTC_FULLTEXT";

	protected Table fulltextTable;

	@Override
	public String getName() {
		return TTC_FULLTEXT;
	}

	@Override
	public boolean accepts(String queryType) {
		return queryType.equals(TTC_FULLTEXT);
	}
	
	@Override
    public Query buildQuery(SQLInfo sqlInfo, Model model,
            PathResolver pathResolver, String query, QueryFilter queryFilter,
            Object... params) throws StorageException {
        Query builtQuery = super.buildQuery(sqlInfo, model, pathResolver, query, queryFilter, params);
        
        String query_ = StringUtils.substringBefore(builtQuery.selectInfo.sql, "ORDER BY");
        String orderBy = StringUtils.substringAfter(builtQuery.selectInfo.sql, "ORDER BY");
        
        query_ = query_.concat(" and hierarchy.id not in (select f.id from fulltext f)").concat(" ORDER BY ").concat(orderBy);
       
        SQLInfoSelect sqlInfoSelect = new SQLInfo.SQLInfoSelect(query_, builtQuery.selectInfo.whatColumns, builtQuery.selectInfo.mapMaker, builtQuery.selectInfo.whereColumns, builtQuery.selectInfo.whatColumns);
        
        builtQuery.selectInfo = sqlInfoSelect;
        
        return builtQuery;
    }

}
