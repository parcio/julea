/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include <glib.h>
#include <gio/gio.h>

#include <julea.h>

#include "server.h"

#include <glib/gprintf.h>


//structures to minimize amount of domains in memory
static GPtrArray* domain_manager; //TODO ref + unref domain_manager on each ref and unref of substructures
static GMutex domain_management_mutex;

static struct RefCountedDomain
{
	struct fid_domain* domain;
	struct fid_eq* domain_eq;
  DomainCategory* category;
	gint ref_count;
};

static struct DomainCategory
{
	struct fi_info* info; //should be dublicated in here
	//TODO sortable numeration structure over RefCountedDomains
  gint ref_count;
};

void
domain_manager_init (void)
{
  domain_manager = g_ptr_array_new ();
  g_mutex_init(&domain_management_mutex);
}


void
domain_manager_fini (void)
{
  g_ptr_array_unref(domain_manager);
  g_mutex_clear(&domain_management_mutex);
}

/**
* Request a domain with given info configuration.
* If no domain with that configuration present, a new one will be created and associated with the fabric given.
* Returns TRUE if success and a referenced counted domain is in the rc_domain argument.
* domain_manager_init needs to be called first.
*/
//TODO Implement //check refcount <= max endpoints of domain
gboolean
domain_request(struct fid_fabric* fabric, struct fi_info* info, JConfiguration* configuration, RefCountedDomain** rc_domain)
{
  gboolean ret;
  DomainCategory* category;

  ret = FALSE;

  if(domain_category_search(info, &category) == TRUE)
  {
    //TODO decide whether domain has space, if so, return domain, increase refcount
    //else call domain_new_internal, increase refcount of cat, add new domain, set a pointer to category in rc_domain
    ret = TRUE;
    goto end;
  }
  else
  {
    category = malloc(sizeof(DomainCategory));
    // TODO call domain_new_internal, add a new category with the new domain, set refcount of cat to 1, set a pointer to category in rc_domain
    domain_new_internal(fabric, info, configuration, rc_domain);
    ret = TRUE;
    goto end;
  }

  end:
  return ret;
}

/**
* Searches manager 0 to domain_manager->len for fitting category.
* if found, return said category + true
* if not found return NULL + false
*/
//TODO implement
gboolean
domain_category_search(struct fi_info* info, DomainCategory** rc_cat)
{
  gboolean ret;

  ret = FALSE;

  end:
  return ret;
}

//TODO sort function with highest ref count in front


void
domain_category_ref (DomainCategory* category)
{
  g_return_if_fail(category != NULL);

	g_atomic_int_inc(&category->ref_count);
  //TODO sort category list
}


void
domain_category_unref (DomainCategory* category)
{
  g_return_if_fail(category != NULL);

  if(g_atomic_int_dec_and_test(&category->ref_count) == TRUE)
  {
    g_ptr_array_remove (domain_manager, category);
    free(category);
  }
  else
  {
    //TODO sort category list structure
  }
}

/**
* Increases refcount of domain
*/
void
domain_ref (RefCountedDomain* rc_domain)
{
	g_return_if_fail(rc_domain != NULL);

  g_atomic_int_inc(&rc_domain->ref_count);

  domain_category_ref(rc_domain->category);
}

/**
* Decreases refcount of domain, frees it if reached 0.
* deletes category from manager if no entries in category left.
*/
void
domain_unref (RefCountedDomain* rc_domain)
{
	g_return_if_fail(rc_domain != NULL);

	if(g_atomic_int_dec_and_test(&rc_domain->ref_count) == TRUE)
	{
		int error;

		error = 0;

		g_mutex_lock(&domain_management_mutex);
		error = fi_close(&rc_domain->domain_eq->fid);
		if(error != 0)
		{
			g_critical("\nProblem closing domain event queue of active endpoint on server.\n Details:\n %s", fi_strerror(error));
			error = 0;
		}
		error = fi_close(&rc_domain->domain->fid);
		if(error != 0)
		{
			g_critical("\nProblem closing domain of active endpoint on server.\n Details:\n %s", fi_strerror(error));
		}
		domain_category_unref(rc_domain->category);
		free(rc_domain);
		g_mutex_unlock(&domain_management_mutex);
	}
}


gboolean
domain_new_internal (struct fid_fabric* fabric, struct fi_info* info, JConfiguration* configuration, RefCountedDomain** rc_domain)
{
  gboolean ret;
  int error;
  struct fi_eq_attr event_queue_attr;

  error = 0;
  ret = FALSE;
  event_queue_attr = * j_configuration_get_fi_eq_attr(configuration);
  * rc_domain = malloc(sizeof(RefCountedDomain));


  error = fi_domain(fabric, info, &(* rc_domain)->domain, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating domain for active Endpoint.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_eq_open(fabric, &event_queue_attr, &(* rc_domain)->domain_eq, NULL);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while creating Domain Event queue.\n Details:\n %s", fi_strerror(error));
		goto end;
	}
	error = fi_domain_bind((* rc_domain)->domain, &(* rc_domain)->domain_eq->fid, 0);
	if(error != 0)
	{
		g_critical("\nError occurred on Server while binding Event queue to Domain.\n Details:\n %s", fi_strerror(error));
		goto end;
	}

  (* rc_domain)->category = NULL;

  ret = TRUE;

  end:
  return ret;
}


//Compares function for different domain_attr
//TODO use this to manage domains for less than 1 domain per thread
gboolean
compare_domain_infos(struct fi_info* info1, struct fi_info* info2)
{
	gboolean ret = FALSE;
	struct fi_domain_attr* domain_attr1 = info1->domain_attr;
	struct fi_domain_attr* domain_attr2 = info2->domain_attr;

	if(strcmp(domain_attr1->name, domain_attr2->name) != 0) goto end;
	if(domain_attr1->threading != domain_attr2->threading) goto end;
	if(domain_attr1->control_progress != domain_attr2->control_progress) goto end;
	if(domain_attr1->data_progress != domain_attr2->data_progress) goto end;
	if(domain_attr1->resource_mgmt != domain_attr2->resource_mgmt) goto end;
	if(domain_attr1->av_type != domain_attr2->av_type) goto end;
	if(domain_attr1->mr_mode != domain_attr2->mr_mode) goto end;
	if(domain_attr1->mr_key_size != domain_attr2->mr_key_size) goto end;
	if(domain_attr1->cq_data_size != domain_attr2->cq_data_size) goto end;
	if(domain_attr1->cq_cnt != domain_attr2->cq_cnt) goto end;
	if(domain_attr1->ep_cnt != domain_attr2->ep_cnt) goto end;
	if(domain_attr1->tx_ctx_cnt != domain_attr2->tx_ctx_cnt) goto end;
	if(domain_attr1->rx_ctx_cnt != domain_attr2->rx_ctx_cnt) goto end;
	if(domain_attr1->max_ep_tx_ctx != domain_attr2->max_ep_rx_ctx) goto end;
	if(domain_attr1->max_ep_rx_ctx != domain_attr2->max_ep_rx_ctx) goto end;
	if(domain_attr1->max_ep_srx_ctx != domain_attr2->max_ep_srx_ctx) goto end;
	if(domain_attr1->max_ep_stx_ctx != domain_attr2->max_ep_stx_ctx) goto end;
	if(domain_attr1->cntr_cnt != domain_attr2->cntr_cnt) goto end;
	if(domain_attr1->mr_iov_limit != domain_attr2->mr_iov_limit) goto end;
	if(domain_attr1->caps != domain_attr2->caps) goto end;
	if(domain_attr1->mode != domain_attr2->mode) goto end;
	//if(domain_attr1->auth_key != domain_attr2->auth_key) goto end; //PERROR: wrong comparison
	if(domain_attr1->auth_key_size != domain_attr2->auth_key_size) goto end;
	if(domain_attr1->max_err_data != domain_attr2->max_err_data) goto end;
	if(domain_attr1->mr_cnt != domain_attr2->mr_cnt) goto end;

	ret = TRUE;
	end:
	return ret;
}
