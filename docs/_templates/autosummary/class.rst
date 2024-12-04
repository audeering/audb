{{ objname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

{% block methods %}
{%- for item in (all_methods + attributes)|sort %}
    {%- if not item.startswith('_') or item in hidden_methods %}
        {%- if item in all_methods and objname != 'config' %}
{{ (item + '()') | escape | underline(line='-') }}
.. automethod:: {{ name }}.{{ item }}
        {%- elif item in attributes %}
{{ item | escape | underline(line='-') }}
.. autoattribute:: {{ name }}.{{ item }}
        {%- endif %}
    {% endif %}
{%- endfor %}
{% endblock %}
