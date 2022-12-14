
-- get unit count by zipcode
select
    zipcode,
    count(*)
from housing_units
group by zipcode
order by count(*) desc


-- get requests by status
select status_code, count(*)
from housing_requests
where ip_id = 4 and environment = 'load_test'
group by status_code order by count(*) desc

-- get endpoint with the most requests
select domain, endpoint, count(*)
from housing_requests
where ip_id = 4 and environment = 'load_test'
group by domain, endpoint
order by count(*) desc
limit 5

-- get request rate
with request_data as (
    select
        min(created_at) as start_time,
        max(created_at) as end_time,
        count(*) as request_count
    from housing_requests
    where ip_id = 4 and environment = 'load_test'
) select
    round(extract(epoch from end_time) - extract(epoch from start_time))/60 as duration_min,
    request_count,
    request_count / (extract(epoch from end_time) - extract(epoch from start_time)) as rps
from request_data

-- get latency report
with latency_by_request as (
    select 
        created_at,
        finished_at,
        extract(epoch from finished_at) - extract(epoch from created_at) as latency,
        domain,
        endpoint
    from housing_requests
    where ip_id = 4 and environment = 'load_test'
        and status_code = 200
    order by created_at asc
) select
    min(latency),
    max(latency),
    avg(latency)
from latency_by_request

-- get latency over time from random request sample
with request_sample as (
    select 
        created_at,
        finished_at,
        extract(epoch from finished_at) - extract(epoch from created_at) as latency
    from housing_requests
    where ip_id = 4 and environment = 'load_test'
        and status_code = 200
    order by random()
    limit 10
) select
    created_at,
    latency
from request_sample
order by created_at asc

-- get latency over time by endpoint for highest latency endpoints
with latency_by_request as (
    select 
        created_at,
        finished_at,
        extract(epoch from finished_at) - extract(epoch from created_at) as latency,
        domain,
        endpoint
    from housing_requests
    where ip_id = 4 and environment = 'load_test'
        and status_code = 200
    order by created_at asc
) select
    domain,
    endpoint,
    avg(latency) as avg_latency,
    array_agg(latency) as latencies
from latency_by_request
group by domain, endpoint
order by  avg(latency) desc
limit 5

-- get num_results over time for search requests
select
    -- domain,
    -- endpoint,
    response_info ->> 'search_page_num' as search_page_num,
    array_agg(response_info ->> 'search_num_results') as search_num_results
from housing_requests
where ip_id = 4 and environment = 'load_test'
    and response_info::jsonb ? 'search_page_num'
group by domain, endpoint, response_info ->> 'search_page_num'
limit 5

-- get total count of search requests
select
    count(*) as search_requests,
    count(distinct endpoint) as search_endpoints
from housing_requests
where ip_id = 4 and environment = 'load_test'
    and response_info::jsonb ? 'search_page_num'

-- get any search where number of results varied over time, ordered by most missing results
with search_request_data as (
    select
        domain,
        endpoint,
        response_info ->> 'search_page_num' as search_page_num,
        array_agg(response_info ->> 'search_num_results') as search_num_results,
        avg((response_info ->> 'search_num_results')::integer) as avg_num_results
    from housing_requests
    where ip_id = 4 and environment = 'load_test'
        and response_info::jsonb ? 'search_page_num'
    group by domain, endpoint, response_info ->> 'search_page_num'
) select
    requests.domain,
    requests.endpoint,
    avg_num_results,
    array_length(search_request_data.search_num_results, 1) as num_requests,
    search_request_data.search_num_results as all_requests_num_results,
    requests.response_info->>'search_num_results' as this_request_num_results,
    avg_num_results - (requests.response_info->>'search_num_results')::integer as missing_results
from search_request_data join housing_requests requests
    on search_request_data.domain = requests.domain and
        search_request_data.endpoint = requests.endpoint and
        search_request_data.search_page_num = requests.response_info->>'search_page_num'
where requests.ip_id = 4 and requests.environment = 'load_test'
    and (requests.response_info->>'search_num_results')::integer != search_request_data.avg_num_results
order by avg_num_results - (requests.response_info->>'search_num_results')::integer desc
limit 5

-- get summary stats of searchs where results varied over time
with search_request_data as (
    select
        domain,
        endpoint,
        response_info ->> 'search_page_num' as search_page_num,
        array_agg(response_info ->> 'search_num_results') as search_num_results,
        avg((response_info ->> 'search_num_results')::integer) as avg_num_results
    from housing_requests
    where ip_id = 4 and environment = 'load_test'
        and response_info::jsonb ? 'search_page_num'
    group by domain, endpoint, response_info ->> 'search_page_num'
) select
    min(avg_num_results - (requests.response_info->>'search_num_results')::integer) as max_extra_results,
    max(avg_num_results - (requests.response_info->>'search_num_results')::integer) as max_missing_results,
    count(*) as requests_with_differing_results,
    count(distinct search_request_data.endpoint) as search_endpoints_with_differing_results
from search_request_data join housing_requests requests
    on search_request_data.domain = requests.domain and
        search_request_data.endpoint = requests.endpoint and
        search_request_data.search_page_num = requests.response_info->>'search_page_num'
where requests.ip_id = 4 and requests.environment = 'load_test'
    and (requests.response_info->>'search_num_results')::integer != search_request_data.avg_num_results