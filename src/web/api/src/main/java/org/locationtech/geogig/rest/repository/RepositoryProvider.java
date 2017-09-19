/* Copyright (c) 2014-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.rest.repository;

import java.util.Iterator;
import java.util.Map;

import org.locationtech.geogig.repository.Repository;

import com.google.common.base.Optional;

public interface RepositoryProvider {

    static final String GEOGIG_ROUTE_PREFIX_PROPERTY = "geogig.route.prefix";

    /**
     * Default route prefix is /geogig. This is because there have been issues configuring this
     * prefix in geoserver, so for now it is easier to default it to what is needed there and
     * override it in geogig itself.
     */
    static final String GEOGIG_ROUTE_PREFIX = "${" + GEOGIG_ROUTE_PREFIX_PROPERTY + ":/geogig}";

    static final String BASE_REPOSITORY_ROUTE = "repos";

    /**
     * Key used too lookup the {@link RepositoryProvider} instance in the
     * {@link Request#getAttributes() request attributes}
     */
    String KEY = "__REPOSITORY_PROVIDER_KEY__";

    public Optional<Repository> getGeogig(final String repositoryName);

    /**
     * Indicates if this provider already has a GeoGig Repository with a given name. If this method
     * returns true, it is implied that calling {@link #getGeogig(java.lang.String)} will return a
     * GeoGig Repository that already exists.
     *
     * @param repositoryName The name of the repository.
     * @return True if a GeoGig Repository with the supplied name already exists that this provider
     * can provide, false otherwise.
     */
    public boolean hasGeoGig(final String repositoryName);

    public Repository createGeogig(final String repositoryName,
            final Map<String, String> parameters);

    /**
     * Deletes the repository that matches the given repository name.
     * <p>
     * Implementation detail: the repository instance is removed from the provider's cache.
     */
    void delete(String repoName);

    /**
     * Signals to the repository provider that a repository may no longer be valid.
     */
    void invalidate(String repoName);

    /**
     * @return an Iterator that walks through the names of all the repositories provided.
     */
    public Iterator<String> findRepositories();
}