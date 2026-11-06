{{ config(severity='warn') }}

select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
left join {{ ref('zip_state_reference') }} z
    on u.zip_code = z.zip_code
    and u.state = z.state
where z.zip_code is null
