
d3.json('data.json', function(error, data) {
    var columns = d3.selectAll('.column')[0];
    var capacity = Math.ceil(data.length / columns.length);
    data.forEach(function(item, index) {
	var column = columns[Math.floor(index / capacity)];
	column = d3.select(column);
	var card = column.append('div')
	    .attr('class', 'card')
	card.append('a')
	    .attr('href', 'https://vk.com/club' + item.id)
	    .text(item.name)
	var related = card.append('table')
	    .attr('class', 'related')
	item.related.forEach(function(item) {
	    var row = related.append('tr')
	    row.append('td')
		.attr('class', 'relevance')
		.text((item.weight * 100).toFixed(2) + '%')
	    row.append('td')
		.append('a')
		.attr('href', 'https://vk.com/club' + item.id)
		.text(item.name)
	});
    });
});
