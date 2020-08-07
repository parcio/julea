/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
 * Copyright (C) 2019 Benjamin Warnke
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <julea-config.h>

#include <julea.h>

#include <j_fi_domain_manager.h>

#include <glib/gprintf.h>

#include <stdio_ext.h>

/**
* inits the outer level of the domain_manager
*/
DomainManager*
domain_manager_init(void)
{
	DomainManager* domain_manager;

	domain_manager = malloc(sizeof(DomainManager));
	g_mutex_init(&domain_manager->dm_mutex);
	domain_manager->cat_list = g_ptr_array_new();
	return domain_manager;
}

/**
* shuts the outer level of the domain_manager down
*/
void
domain_manager_fini(DomainManager* domain_manager)
{
	g_ptr_array_unref(domain_manager->cat_list);
	g_mutex_clear(&domain_manager->dm_mutex);
	free(domain_manager);
}

/**
* Request a domain with given info configuration.
* If no domain with that configuration present, a new one will be created and associated with the fabric given.
* Returns TRUE if success and a referenced counted domain is in the rc_domain argument.
* domain_manager_init needs to be called first.
*/
gboolean
domain_request(struct fid_fabric* fabric,
	       struct fi_info* info,
	       JConfiguration* configuration,
	       RefCountedDomain** rc_domain,
	       DomainManager* domain_manager,
	       const gchar* location)
{
	gboolean ret;
	DomainCategory* category;

	ret = FALSE;

	if (domain_category_search(info, &category, domain_manager))
	{
		GSList* current;

		current = category->domain_list;
		while (current != NULL)
		{
			if (((RefCountedDomain*)current->data)->ref_count < category->info->domain_attr->ep_cnt
			    || ((RefCountedDomain*)current->data)->ref_count * 2 < category->info->domain_attr->cq_cnt) // 2 cqs per endpoint
			{
				(*rc_domain) = (RefCountedDomain*)current->data;
				domain_ref((*rc_domain));
				break;
			}
			else
			{
				current = category->domain_list->next;
			}
		}
		//if end of list reached without available space in any domain, create new domain
		if (current == NULL)
		{
			if (!domain_new_internal(fabric, info, configuration, category, rc_domain, location))
			{
				g_critical("\nProblem initiating a new domain on %s.", location);
				goto end;
			}
			category->domain_list = g_slist_append(category->domain_list, (*rc_domain));
			domain_category_ref(category);
		}
		ret = TRUE;
	}
	else
	{
		if (!domain_category_new_internal(fabric, info, configuration, &category, rc_domain, domain_manager, location))
		{
			g_critical("\nProblem initiating a new domain category on %s.", location);
			goto end;
		}
		ret = TRUE;
	}

end:
	return ret;
}

/**
* Searches manager for fitting category.
* if found, return said category + true
* if not found return NULL + false
*/
gboolean
domain_category_search(struct fi_info* info, DomainCategory** rc_cat, DomainManager* domain_manager)
{
	gboolean ret;
	DomainCategory* tmp_cat;

	(*rc_cat) = NULL;
	tmp_cat = NULL;
	ret = FALSE;

	g_mutex_lock(&domain_manager->dm_mutex);
	for (guint i = 0; i < domain_manager->cat_list->len; i++)
	{
		tmp_cat = g_ptr_array_index(domain_manager->cat_list, i);
		if (compare_domain_infos(info, tmp_cat->info))
		{
			(*rc_cat) = tmp_cat;
			ret = TRUE;
			break;
		}
	}

	g_mutex_unlock(&domain_manager->dm_mutex);
	return ret;
}

/**
* increases ref_count of a DomainCategory
*/
void
domain_category_ref(DomainCategory* category)
{
	g_return_if_fail(category != NULL);

	g_atomic_int_inc(&category->ref_count);
}

/**
* Decreases ref_count of a DomainCategory. If ref_count reaches 0, structure is freed and ref_count of domain_manager is reduced
*/
void
domain_category_unref(DomainCategory* category, DomainManager* domain_manager)
{
	g_return_if_fail(category != NULL);

	if (g_atomic_int_dec_and_test(&category->ref_count) == TRUE)
	{
		fi_freeinfo(category->info);
		g_slist_free(category->domain_list);
		g_ptr_array_remove(domain_manager->cat_list, category);
		g_ptr_array_unref(domain_manager->cat_list);
		free(category);
	}
}

/**
* Increases ref_count of domain
*/
void
domain_ref(RefCountedDomain* rc_domain)
{
	g_return_if_fail(rc_domain != NULL);

	g_atomic_int_inc(&rc_domain->ref_count);
}

/**
* Decreases ref_count of domain, frees it if reached 0.
* deletes category from manager if no entries in category left.
*/
void
domain_unref(RefCountedDomain* rc_domain, DomainManager* domain_manager, const gchar* location)
{
	g_return_if_fail(rc_domain != NULL);
	/*
  g_printf("\nrc_domain->ref_count %d\n", rc_domain->ref_count);
  g_printf("domain_manager->cat_list->len: %d\n", domain_manager->cat_list->len);
  g_printf("rc_domain->category->domain_list size: %d\n", g_slist_length(rc_domain->category->domain_list));
  */
	if (g_atomic_int_dec_and_test(&rc_domain->ref_count) == TRUE)
	{
		int error;

		error = 0;

		error = fi_close(&rc_domain->domain_eq->fid);
		if (error != 0)
		{
			g_critical("\nProblem closing domain event queue of active endpoint on %s.\n Details:\n %s", fi_strerror(abs(error)), location);
			error = 0;
		}
		error = fi_close(&rc_domain->domain->fid);
		if (error != 0)
		{
			g_critical("\nProblem closing domain of active endpoint on %s.\n Details:\n %s", fi_strerror(abs(error)), location);
		}
		domain_category_unref(rc_domain->category, domain_manager);
		free(rc_domain);
	}
}

/**
* Builds a new category with a ref_count of 1 and first domain entry on list and adds it to domain_manager
*/
gboolean
domain_category_new_internal(struct fid_fabric* fabric,
			     struct fi_info* info,
			     JConfiguration* configuration,
			     DomainCategory** category,
			     RefCountedDomain** rc_domain,
			     DomainManager* domain_manager,
			     const gchar* location)
{
	gboolean ret;

	ret = FALSE;

	(*category) = malloc(sizeof(DomainCategory));
	(*category)->domain_list = NULL;
	(*category)->info = fi_dupinfo(info);
	(*category)->ref_count = 0;
	if (!domain_new_internal(fabric, info, configuration, (*category), rc_domain, location))
	{
		g_critical("domain_new_internal failed on %s", location);
		goto end;
	}
	(*category)->domain_list = g_slist_append((*category)->domain_list, (*rc_domain));

	g_ptr_array_add(domain_manager->cat_list, (gpointer)(*category));
	g_ptr_array_ref(domain_manager->cat_list);

	ret = TRUE;
end:
	return ret;
}

/**
* Allocates a new reference counted domain (with internal objects) with a ref_count of 1
*/
gboolean
domain_new_internal(struct fid_fabric* fabric,
		    struct fi_info* info,
		    JConfiguration* configuration,
		    DomainCategory* category,
		    RefCountedDomain** rc_domain,
		    const gchar* location)
{
	gboolean ret;
	int error;
	struct fi_eq_attr event_queue_attr;

	error = 0;
	ret = FALSE;
	event_queue_attr = *j_configuration_get_fi_eq_attr(configuration);
	*rc_domain = malloc(sizeof(RefCountedDomain));

	if (info->domain_attr->ep_cnt < 6)
	{
		g_warning("\nLibfabric tries to allocate a new domain with %ld maximal connections on %s. Max connections supported by underlying hardware possibly reached.", info->domain_attr->ep_cnt - g_slist_length(category->domain_list), location);
	}

	error = fi_domain(fabric, info, &(*rc_domain)->domain, NULL);
	if (error != 0)
	{
		g_critical("\nError occurred on %s while creating domain for active Endpoint.\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}
	error = fi_eq_open(fabric, &event_queue_attr, &(*rc_domain)->domain_eq, NULL);
	if (error != 0)
	{
		g_critical("\nError occurred on %s while creating Domain Event queue.\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}
	error = fi_domain_bind((*rc_domain)->domain, &(*rc_domain)->domain_eq->fid, 0);
	if (error != 0)
	{
		g_critical("\nError occurred on %s while binding Event queue to Domain.\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}

	(*rc_domain)->ref_count = 1;
	(*rc_domain)->category = category;
	domain_category_ref(category);

	ret = TRUE;

end:
	return ret;
}

/**
*Compares if 2 fi_info structures if they contain the same attributes for domains
*/
gboolean
compare_domain_infos(struct fi_info* info1, struct fi_info* info2)
{
	gboolean ret;
	struct fi_domain_attr* domain_attr1;
	struct fi_domain_attr* domain_attr2;

	if (info1 == info2)
	{
		ret = TRUE;
		goto end;
	}

	ret = FALSE;
	domain_attr1 = info1->domain_attr;
	domain_attr2 = info2->domain_attr;

	if (strcmp(domain_attr1->name, domain_attr2->name) != 0)
		goto end;
	if (domain_attr1->threading != domain_attr2->threading)
		goto end;
	if (domain_attr1->control_progress != domain_attr2->control_progress)
		goto end;
	if (domain_attr1->data_progress != domain_attr2->data_progress)
		goto end;
	if (domain_attr1->resource_mgmt != domain_attr2->resource_mgmt)
		goto end;
	if (domain_attr1->av_type != domain_attr2->av_type)
		goto end;
	if (domain_attr1->mr_mode != domain_attr2->mr_mode)
		goto end;
	if (domain_attr1->mr_key_size != domain_attr2->mr_key_size)
		goto end;
	if (domain_attr1->cq_data_size != domain_attr2->cq_data_size)
		goto end;
	if (domain_attr1->cq_cnt != domain_attr2->cq_cnt)
		goto end;
	if (domain_attr1->ep_cnt != domain_attr2->ep_cnt)
		goto end;
	if (domain_attr1->tx_ctx_cnt != domain_attr2->tx_ctx_cnt)
		goto end;
	if (domain_attr1->rx_ctx_cnt != domain_attr2->rx_ctx_cnt)
		goto end;
	if (domain_attr1->max_ep_tx_ctx != domain_attr2->max_ep_tx_ctx)
		goto end;
	if (domain_attr1->max_ep_rx_ctx != domain_attr2->max_ep_rx_ctx)
		goto end;
	if (domain_attr1->max_ep_srx_ctx != domain_attr2->max_ep_srx_ctx)
		goto end;
	if (domain_attr1->max_ep_stx_ctx != domain_attr2->max_ep_stx_ctx)
		goto end;
	if (domain_attr1->cntr_cnt != domain_attr2->cntr_cnt)
		goto end;
	if (domain_attr1->mr_iov_limit != domain_attr2->mr_iov_limit)
		goto end;
	if (domain_attr1->caps != domain_attr2->caps)
		goto end;
	if (domain_attr1->mode != domain_attr2->mode)
		goto end;
	if (domain_attr1->auth_key_size != domain_attr2->auth_key_size)
		goto end;
	if (domain_attr1->auth_key_size > 0)
	{
		if (memcmp(domain_attr1->auth_key, domain_attr2->auth_key, domain_attr1->auth_key_size) != 0)
			goto end;
	}
	if (domain_attr1->max_err_data != domain_attr2->max_err_data)
		goto end;
	if (domain_attr1->mr_cnt != domain_attr2->mr_cnt)
		goto end;

	ret = TRUE;
end:
	return ret;
}
