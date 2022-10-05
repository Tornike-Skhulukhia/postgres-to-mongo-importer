select cn.name as country, cl.language, cl.percentage, cl.isofficial, cn.continent, code as country_code
from country as cn
join countrylanguage as cl
on cn.code = cl.countrycode 
order by country asc, isofficial desc;

