import traceback
import traceback

def tester():
    pass
action = None
try:
    try:
        action = lambda: "aaa"
        tester(33)
    except TypeError as err:
        action_error = (
            f'Failed to execute action @ File "{action.__code__.co_filename}", line {action.__code__.co_firstlineno}'
        )
        raise Exception(action_error + ": " + str(err))
except Exception as ex:
    print("\n".join(traceback.format_exception(ex.__class__, ex, ex.__traceback__)))
