/*
 * (C) Copyright 2012 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.opentoutatice.fulltext.reindex;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.core.api.AbstractSession;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.IterableQueryResult;
import org.nuxeo.ecm.core.api.NuxeoPrincipal;
import org.nuxeo.ecm.core.event.EventService;
import org.nuxeo.ecm.core.query.QueryFilter;
import org.nuxeo.ecm.core.query.sql.NXQL;
import org.nuxeo.ecm.core.storage.FulltextConfiguration;
import org.nuxeo.ecm.core.storage.sql.Model;
import org.nuxeo.ecm.core.storage.sql.Node;
import org.nuxeo.ecm.core.storage.sql.Session;
import org.nuxeo.ecm.core.storage.sql.SessionImpl;
import org.nuxeo.ecm.core.storage.sql.SimpleProperty;
import org.nuxeo.ecm.core.storage.sql.coremodel.SQLFulltextExtractorWork;
import org.nuxeo.ecm.core.storage.sql.coremodel.SQLSession;
import org.nuxeo.ecm.core.storage.sql.ra.ConnectionImpl;
import org.nuxeo.ecm.core.work.api.Work;
import org.nuxeo.ecm.core.work.api.WorkManager;
import org.nuxeo.ecm.core.work.api.WorkManager.Scheduling;
import org.nuxeo.ecm.webengine.jaxrs.session.SessionFactory;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.transaction.TransactionHelper;
import org.opentoutatice.fulltext.reindex.query.FulltextQueryMaker;

/**
 * JAX-RS component used to do fulltext reindexing of the whole database.
 *
 * @since 5.6
 */
@Path("reindexFulltext")
public class ReindexFulltextRoot {

	public static Log log = LogFactory.getLog(ReindexFulltextRoot.class);

	protected static final String DC_TITLE = "dc:title";

	protected static final int DEFAULT_BATCH_SIZE = 100;

	@Context
	protected HttpServletRequest request;

	public CoreSession coreSession;

	protected Session session;

	protected FulltextConfiguration fulltextConfiguration;

	protected static class Info {
		public final Serializable id;

		public final String type;

		public Info(Serializable id, String type) {
			this.id = id;
			this.type = type;
		}
	}

	public class Cron {
		public int startHour;
		public int endHour;

		public static final String SEPARATOR = "-";
		public static final String ERROR_MSG = "[%s]: bad cron parameter format - must be 'startHour-endHour' with startHour and endHour integer and not equal";

		public Cron(int startHour, int endHour) {
			super();
			this.startHour = startHour;
			this.endHour = endHour;
		}

	}

	@GET
	public String get(@QueryParam("batchSize") int batchSize, @QueryParam("batch") int batch,
			@QueryParam("year") int year, @QueryParam("cron") String cron) throws Exception {
		coreSession = SessionFactory.getSession(request);
		Cron cron_ = getCron(cron);
		return reindexFulltext(batchSize, batch, year, cron_);
	}

	protected Cron getCron(String cron_) {
		Cron cron = null;

		if (cron_ != null) {
			if (StringUtils.contains(cron_, Cron.SEPARATOR)) {
				String[] interval = StringUtils.split(cron_, Cron.SEPARATOR);

				if (interval.length == 2 && StringUtils.isNotBlank(interval[0])
						&& StringUtils.isNotBlank(interval[1])) {

					Integer startHour = null;
					Integer endHour = null;
					try {
						startHour = Integer.valueOf(interval[0]);
						endHour = Integer.valueOf(interval[1]);
					} catch (NumberFormatException nfe) {
						throw new ClientException(String.format(Cron.ERROR_MSG, cron_));
					}

					if (0 <= startHour && startHour <= 23 && 0 <= endHour && endHour <= 23
							&& !startHour.equals(endHour)) {
						cron = new Cron(startHour, endHour);
					}
				} else {
					throw new ClientException(String.format(Cron.ERROR_MSG, cron_));
				}
			} else {
				throw new ClientException(String.format(Cron.ERROR_MSG, cron_));
			}
		}

		return cron;
	}

	/**
	 * Launches a fulltext reindexing of the database.
	 *
	 * @param batchSize the batch size, defaults to 100
	 * @param batch     if present, the batch number to process instead of all
	 *                  batches; starts at 1
	 * @return when done, ok + the total number of docs
	 */
	public String reindexFulltext(int batchSize, int batch, int year, Cron cron) throws Exception {
		Principal principal = coreSession.getPrincipal();
		if (!(principal instanceof NuxeoPrincipal)) {
			return "unauthorized";
		}
		NuxeoPrincipal nuxeoPrincipal = (NuxeoPrincipal) principal;
		if (!nuxeoPrincipal.isAdministrator()) {
			return "unauthorized";
		}

		log("Reindexing starting");
		if (batchSize <= 0) {
			batchSize = DEFAULT_BATCH_SIZE;
		}
		List<Info> infos = getInfos(year);
		int size = infos.size();
		int numBatches = (size + batchSize - 1) / batchSize;
		if (batch < 0 || batch > numBatches) {
			batch = 0; // all
		}
		batch--;

		log("Reindexing of %s documents, batch size: %s, number of batches: %s", size, batchSize, numBatches);
		if (batch >= 0) {
			log("Reindexing limited to batch: %s", batch + 1);
		}

		long begin_ = System.currentTimeMillis();

		boolean tx = TransactionHelper.isTransactionActive();
		if (tx) {
			TransactionHelper.commitOrRollbackTransaction();
		}

		int nb = 0;
		int n = 0;
		int errs = 0;
		for (int i = 0; i < numBatches; i++) {
			if (batch >= 0 && batch != i) {
				continue;
			}
			int pos = i * batchSize;
			int end = pos + batchSize;
			if (end > size) {
				end = size;
			}
			List<Info> batchInfos = infos.subList(pos, end);
			log("Reindexing batch %s/%s, first id: %s", i + 1, numBatches, batchInfos.get(0).id);
			try {
				if (validCron(cron)) {
					doBatch(batchInfos);
					nb += batchInfos.size();
				} else {
					log.warn(String.format("Current hour [%s] not in CRON interval [%s-%s[: batchs processing stopped",
							String.valueOf(GregorianCalendar.getInstance().get(Calendar.HOUR_OF_DAY)),
							String.valueOf(cron.startHour), String.valueOf(cron.endHour)));
					break;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				int nbBatch = i + 1;
				log.error("Error processing batch " + nbBatch, e);
				errs++;
			}
			n += end - pos;
		}

		long duration_ = System.currentTimeMillis() - begin_;
		log("Reindexing of [%s] done in [%s] ms", String.valueOf(nb), String.valueOf(duration_));
		double speed = nb > 0 && duration_ > 0 ? (nb * 3600000 / duration_) : 0;
		log("Average speed: [%s] docs/h", String.valueOf(speed));

		if (tx) {
			TransactionHelper.startTransaction();
		}
		return "done: " + n + " total: " + size + " batch_errors: " + errs;
	}

	protected List<Info> getInfos(int year) throws Exception {
		getLowLevelSession();
		List<Info> infos = new ArrayList<Info>();
		String query = "SELECT ecm:uuid, ecm:primaryType FROM Document WHERE ecm:mixinType <> 'NotFulltextIndexable' AND ecm:isProxy = 0"
				+ " AND ecm:currentLifeCycleState <> 'deleted'";
		if (year > 0) {
			query += String.format(" AND dc:created >= DATE '%s-01-01'", String.valueOf(year));
		}
		query += " ORDER BY dc:created, ecm:uuid DESC";
		IterableQueryResult it = session.queryAndFetch(query, FulltextQueryMaker.TTC_FULLTEXT, QueryFilter.EMPTY);
		try {
			for (Map<String, Serializable> map : it) {
				Serializable id = map.get(NXQL.ECM_UUID);
				String type = (String) map.get(NXQL.ECM_PRIMARYTYPE);
				infos.add(new Info(id, type));
			}
		} finally {
			it.close();
		}
		return infos;
	}

	protected boolean validCron(Cron cron) {
		boolean valid = cron == null;

		if (cron != null) {
			int currentHour = GregorianCalendar.getInstance().get(Calendar.HOUR_OF_DAY);
			if (cron.startHour < cron.endHour) {
				valid = cron.startHour <= currentHour && currentHour < cron.endHour;
			} else {
				valid = (cron.startHour <= currentHour && currentHour > cron.endHour)
						|| (cron.startHour >= currentHour && currentHour < cron.endHour);
			}
		}

		return valid;
	}

	protected void doBatch(List<Info> infos) throws Exception {
		boolean tx;
		boolean ok;

		// transaction for the sync batch
		tx = TransactionHelper.startTransaction();

		getLowLevelSession(); // for fulltextInfo
		List<Serializable> ids = new ArrayList<Serializable>(infos.size());
		Set<String> asyncIds = new HashSet<String>();
		Model model = session.getModel();
		for (Info info : infos) {
			ids.add(info.id);
			if (fulltextConfiguration.isFulltextIndexable(info.type)) {
				asyncIds.add(model.idToString(info.id));
			}
		}
		ok = false;
		try {
			runSimpleTextBatch(ids, asyncIds);
			ok = true;
		} finally {
			if (tx) {
				if (!ok) {
					TransactionHelper.setTransactionRollbackOnly();
					log.error("Rolling back simple text reindexing");
				}
				TransactionHelper.commitOrRollbackTransaction();
			}
		}

		runBinaryTextBatch(asyncIds);

		// wait for async completion after transaction commit
		Framework.getLocalService(EventService.class).waitForAsyncCompletion();
	}

	/*
	 * Do this at the low-level session level because we may have to modify things
	 * like versions which aren't usually modifiable, and it's also good to bypass
	 * all listeners.
	 */
	protected void runSimpleTextBatch(List<Serializable> ids, Set<String> asyncIds) throws Exception {
//		getLowLevelSession();
//		session.getNodesByIds(new ArrayList<Serializable>(ids)); // batch fetch
//
//		WorkManager workManager = Framework.getService(WorkManager.class);
//		Model model = session.getModel();
//		String repositoryName = coreSession.getRepositoryName();
//
//		for (Serializable id : ids) {
//
//			if (id == null) {
//				// cannot happen, but has been observed :(
//				log.error("Got null doc id in fulltext update, cannot happen");
//				continue;
//			}
//
//			Node node = session.getNodeById(id);
//			if (node != null) {
//				String documentType = node.getPrimaryType();
//				String[] mixinTypes = node.getMixinTypes();
//
////				if (!fulltextConfiguration.isFulltextIndexable(documentType)) {
////					continue;
////				}
//
//				if (asyncIds.contains(id)) {
//					node.getSimpleProperty(Model.FULLTEXT_JOBID_PROP).setValue(model.idToString(node.getId()));
//				}
//
//				FulltextFinder fulltextFinder = new FulltextFinder(new DefaultFulltextParser(), node,
//						(SessionImpl) session);
//				List<IndexAndText> indexesAndText = new LinkedList<IndexAndText>();
//				for (String indexName : fulltextConfiguration.indexNames) {
//					Set<String> paths;
//					if (fulltextConfiguration.indexesAllSimple.contains(indexName)) {
//						// index all string fields, minus excluded ones
//						// TODO XXX excluded ones...
//						paths = model.getSimpleTextPropertyPaths(documentType, mixinTypes);
//					} else {
//						// index configured fields
//						paths = fulltextConfiguration.propPathsByIndexSimple.get(indexName);
//					}
//					String text = fulltextFinder.findFulltext(paths);
//					indexesAndText.add(new IndexAndText(indexName, text));
//				}
//				if (!indexesAndText.isEmpty()) {
//					Work work = new FulltextUpdaterWork(repositoryName, model.idToString(id), true, false,
//							indexesAndText);
//					workManager.schedule(work, Scheduling.IF_NOT_SCHEDULED, false);
//				}
//
//			} else {
//				log.warn(String.format("Doc [%s] does not exist anymore", id != null ? id.toString() : "NULL"));
//			}
//
//		}

		getLowLevelSession();

		session.getNodesByIds(ids); // batch fetch

		Map<Serializable, String> titles = new HashMap<Serializable, String>();
		for (Serializable id : ids) {
			Node node = session.getNodeById(id);
			if (asyncIds.contains(id)) {
				node.setSimpleProperty(Model.FULLTEXT_JOBID_PROP, id);
			}
			SimpleProperty prop;
			try {
				prop = node.getSimpleProperty(DC_TITLE);
			} catch (IllegalArgumentException e) {
				continue;
			}
			String title = (String) prop.getValue();
			titles.put(id, title);
			prop.setValue(title + " ");
		}
		session.save();

		for (Serializable id : ids) {
			Node node = session.getNodeById(id);
			SimpleProperty prop;
			try {
				prop = node.getSimpleProperty(DC_TITLE);
			} catch (IllegalArgumentException e) {
				continue;
			}
			prop.setValue(titles.get(id));
		}
		session.save();

	}

	protected void runBinaryTextBatch(Set<String> asyncIds) throws ClientException {
		if (asyncIds.isEmpty()) {
			return;
		}
		String repositoryName = coreSession.getRepositoryName();
		WorkManager workManager = Framework.getLocalService(WorkManager.class);
		for (String id : asyncIds) {
			Work work = new SQLFulltextExtractorWork(repositoryName, id);
			// schedule immediately, we're outside a transaction
			workManager.schedule(work, Scheduling.IF_NOT_SCHEDULED, false);
		}
	}

	protected void log(String format, Object... args) {
		log.info(String.format(format, args));
	}

	/**
	 * This has to be called once the transaction has been started.
	 */
	protected void getLowLevelSession() throws Exception {
		SQLSession s = (SQLSession) ((AbstractSession) coreSession).getSession();
		Field f2 = SQLSession.class.getDeclaredField("session");
		f2.setAccessible(true);
		ConnectionImpl connection = (ConnectionImpl) f2.get(s);
		Field f3 = ConnectionImpl.class.getDeclaredField("session");
		f3.setAccessible(true);
		session = (SessionImpl) f3.get(connection);
		fulltextConfiguration = session.getModel().getFulltextConfiguration();
	}

}
