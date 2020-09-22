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
package org.nuxeo.ecm.core.storage.sql;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.VersioningOption;
import org.nuxeo.ecm.core.api.impl.DocumentModelImpl;
import org.nuxeo.ecm.platform.usermanager.NuxeoPrincipalImpl;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.LocalDeploy;
import org.opentoutatice.fulltext.reindex.ReindexFulltextRoot;



@Deploy({"org.nuxeo.ecm.core.storage.sql"})
@LocalDeploy({"org.nuxeo.ecm.core.storage.sql", "fr.toutatice.ecm.acrennes.fulltext.reindex:core-types-test.xml",
	"org.nuxeo.ecm.core.storage.sql:fulltext-querymaker-test.xml"})
@Ignore
public class TestReindexFulltext extends TXSQLRepositoryTestCase {
	
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // for user schema
        deployBundle("org.nuxeo.ecm.directory.types.contrib");
        // reopen session with admin user
        closeSession();
        NuxeoPrincipalImpl admin = new NuxeoPrincipalImpl("Administrator",
                false, true);
        session = openSessionAs(admin);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testReindexFulltext() throws Exception {
        // create a live doc
        DocumentModel file = new DocumentModelImpl("/", "file", "File");
        file = session.createDocument(file);

        // create a version
        DocumentRef ver = session.checkIn(file.getRef(),
                VersioningOption.MINOR, null);

        // create a proxy (not reindexed)
        session.createProxy(ver, session.getRootDocument().getRef());

        // create an unfiled doc
        DocumentModel file2 = new DocumentModelImpl((String) null, "file2",
                "File");
        session.createDocument(file2);

        session.save();

        ReindexFulltextRoot reindex = new ReindexFulltextRoot();
        reindex.coreSession = session;
        String ok = reindex.reindexFulltext(0, 0, 0, null);
        assertEquals("done: 3 total: 3 batch_errors: 0", ok);
    }

}
