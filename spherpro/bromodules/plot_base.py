from spherpro.bromodules.helpers_varia import HelperDb
class BasePlot(HelperDb):
    def __init__(self, bro):
        super().__init__(bro)


    def _plot_imc(img_id, channel, titel=None, ax=None):
        if ax is None:
            fig = plt.figure(figsize=(20,20))
            ax = plt.gca()
        bro=self.bro
        imc_img = self.get_imc_channel(img_id, channel)
        ax.imshow(imc_img, cmap='Greys_r',
                             norm=colors.SymLogNorm(
                                 linthresh=1, linscale=0.03))
        ax.set_title('{} \n {}'.format(bro.helpers.dbhelp
            .get_target_by_channel(channel), channel))
        ax.axis('off')
        return ax


def adapt_ax_clims(axs):
    caxs = [cax for ax in axs for cax in ax.images]
    clims = [cax.get_clim() for cax in caxs]
    clims = [c for c in clims if c != (True, True)]
    clim_all = [f(c) for f, c in zip([np.min, np.max], zip(*clims))]
    for cax in caxs:
        cax.set_clim(clim_all)


