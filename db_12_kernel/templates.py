COMPLETION_TEMPLATE = """
def __jedi_remote_complete__():
    text = \"\"\"%s\"\"\"
    interpreter = jedi.Interpreter(text, [locals(), globals()])
    name = jedi.api.helpers.get_on_completion_name(interpreter._module_node, %s, %s)
    before = text[:len(text) - len(name)] 

    return (before, name, [c.name_with_symbols for c in interpreter.completions()])
__jedi_remote_complete__()
"""

HELP_TEMPLATE = """
def __remote_help__(last, default):
    parts = last.split('.')
    obj = locals().get(parts[0], None)
    if not obj:
        obj = globals().get(parts[0], None)
        if obj is None:
            return default

    for p in parts[1:]:
        obj = getattr(obj, p, None)
        if not obj:
            return default

    strhelp = pydoc.render_doc(obj)
    return strhelp
__remote_help__('%s', '%s')
"""

SHOW_TEMPLATE = """
def show(format="svg"):
    import base64
    import json
    from io import BytesIO

    output = BytesIO()
    plt.savefig(output, format=format)

    output.seek(0)
    if format in ["png", "jpg", "gif"]:
        data = base64.b64encode(output.read()).decode("utf8")
    else:
        data = output.read().decode("utf8")
    return {"client":"__dbjl_light__", "format": format, "data": data}

import matplotlib.pyplot
matplotlib.pyplot.show = show
"""
