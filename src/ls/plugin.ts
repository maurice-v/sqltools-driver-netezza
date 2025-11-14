import NetezzaDriver from './driver';
import { ILanguageServerPlugin } from '@sqltools/types';
import { DRIVER_ALIASES } from '../constants';

const NetezzaDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server.getContext().drivers.set(value, NetezzaDriver as any);
    });
  }
}

export default NetezzaDriverPlugin;
